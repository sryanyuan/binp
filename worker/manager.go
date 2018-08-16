package worker

import (
	"context"
	"hash/crc32"
	"strings"
	"sync"
	"time"

	"github.com/sryanyuan/binp/utils"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

const (
	minWorkerCount       = 1
	workerReportChanSize = 2560
	workerJobChanSize    = 2560
	rplPointSaveInterval = 15
)

type workerReport struct {
	discardable bool
	wid         int
	err         error
}

// WorkerManager manages all workers
type WorkerManager struct {
	workers          []*worker
	wreport          chan *workerReport
	done             context.Context
	stopFn           context.CancelFunc
	wg               sync.WaitGroup
	jobWg            sync.WaitGroup
	lastRplPointTime int64
}

// NewWorkerManager creates a new WorkerManager
func NewWorkerManager(cfg *WorkerConfig) (*WorkerManager, error) {
	workerCount := cfg.WorkerCount
	if workerCount < minWorkerCount {
		logrus.Warnf("Minimal worker count is %d", minWorkerCount)
		workerCount = minWorkerCount
	}

	wm := &WorkerManager{
		wreport:          make(chan *workerReport, workerReportChanSize),
		workers:          make([]*worker, 0, workerCount),
		lastRplPointTime: time.Now().Unix(),
	}

	// Create executor
	execs, err := createExecutors(cfg.Tos)
	if nil != err {
		return nil, errors.Trace(err)
	}

	for i := 0; i < workerCount; i++ {
		w := &worker{}
		w.wid = i
		w.executors = execs
		w.jobWg = &wm.jobWg
		wm.workers = append(wm.workers, w)
	}

	return wm, nil
}

// Start starts all workers
func (w *WorkerManager) Start() error {
	for _, wr := range w.workers {
		if err := wr.start(&w.wg, 0, 0); nil != err {
			return errors.Trace(err)
		}
	}

	w.wg.Add(1)
	go w.workerReportLoop()

	return nil
}

// Stop stops all workers
func (w *WorkerManager) Stop() {
	w.jobWg.Wait()

	for _, v := range w.workers {
		v.stop()
	}
	// Once all worker is stop, worker report channel won't be used any more
	close(w.wreport)
	w.wg.Wait()
}

func (w *WorkerManager) uploadReport(r *workerReport) {
	defer func() {
		w.wg.Done()
	}()

	select {
	case w.wreport <- r:
		{
			// Nothing
		}
	default:
		{
			// Check discardable
			if r.discardable {
				return
			}
			logrus.Warnf("Worker report channel is full")
			w.wreport <- r
		}
	}
}

func (w *WorkerManager) workerReportLoop() {
	for {
		select {
		case rp, ok := <-w.wreport:
			{
				if !ok {
					logrus.Infof("Worker report loop quit")
					return
				}
				_ = rp
			}
		}
	}
}

// DispatchWorkerEvent dispatchs WorkerEvent to worker, return true if replication point is checked
func (w *WorkerManager) DispatchWorkerEvent(job *WorkerEvent, dispPolicy int) (bool, error) {
	index := -1
	var key string
	if DispatchPolicyPrimaryKey == dispPolicy {
		// Find keys from primary keys
		if len(job.Ti.IndexColumns) != 0 {
			pkvalues := make([]string, 0, len(job.Ti.IndexColumns))
			for _, ic := range job.Ti.IndexColumns {
				pkvalues = append(pkvalues, job.Columns[ic.Index].ValueToString())
			}
			key = strings.Join(pkvalues, ",")
		}
	} else if DispatchPolicyTableName == dispPolicy {
		key = utils.GetTableKey(job.SDesc.RewriteSchema, job.SDesc.RewriteTable)
	}
	if "" == key {
		return false, errors.Errorf("Can't get job dispatch key, dispatch policy = %d, job = %v", dispPolicy, job)
	}
	index = int(crc32.ChecksumIEEE([]byte(key))) % len(w.workers)
	w.workers[index].push(job)
	w.jobWg.Add(1)

	// Need wait and write the lastest replication point
	rplPointChecked := false
	if rplPointChecked = w.needSaveRplPoint(); rplPointChecked {
		w.jobWg.Wait()
		w.lastRplPointTime = time.Now().Unix()
	}

	return rplPointChecked, nil
}

func (w *WorkerManager) needSaveRplPoint() bool {
	tn := time.Now().Unix()
	if tn-w.lastRplPointTime > rplPointSaveInterval {
		return true
	}
	return false
}
