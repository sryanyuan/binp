package worker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

// Dispatch policy
const (
	DispatchPolicyTableName = iota
	DispatchPolicyPrimaryKey
)

const (
	defaultWorkerQueueSize           = 20
	defaultWorkerQueueCommitInterval = 200
)

// Worker status
const (
	WorkerStatusNone = iota
	WorkerStatusRunning
	WorkerStatusAbnormal
	WorkerStatusExit
)

type worker struct {
	wid            int
	wq             *workerQueue
	executors      []IJobExecutor
	wg             *sync.WaitGroup
	jobWg          *sync.WaitGroup
	jobCh          chan *WorkerEvent
	lastCommitTm   int64
	commitInterval int64
	status         int64
}

func (w *worker) start(wg *sync.WaitGroup, wqsz, wqintv int) error {
	if nil == w.executors || 0 == len(w.executors) {
		return errors.New("Executor is nil")
	}
	if nil == w.jobWg {
		return errors.New("Job waitgroup is nil")
	}
	if wqsz <= 0 {
		wqsz = defaultWorkerQueueSize
	}
	if wqintv <= 0 {
		wqintv = defaultWorkerQueueCommitInterval
	}

	w.wg = wg
	w.jobCh = make(chan *WorkerEvent, workerJobChanSize)
	w.wq = newWorkerQueue(wqsz)
	w.lastCommitTm = time.Now().Unix()
	w.commitInterval = int64(wqintv)

	w.wg.Add(1)
	go w.loop()

	return nil
}

func (w *worker) stop() {
	close(w.jobCh)
}

func (w *worker) push(job *WorkerEvent) {
	w.jobCh <- job
}

func (w *worker) loop() {
	var err error

	defer func() {
		w.wg.Done()
		atomic.StoreInt64(&w.status, WorkerStatusExit)
	}()

	atomic.StoreInt64(&w.status, WorkerStatusRunning)

	for {
		select {
		case job, ok := <-w.jobCh:
			{
				if !ok {
					logrus.Infof("Worker %s stop", w.wid)
					return
				}
				// Push into queue and check if full
				w.wq.push(job)
				if w.wq.full() {
					// Do commit job
					if err = w.commitQueue(); nil != err {
						return
					}
				}
			}
		default:
			{
				tms := time.Now().UnixNano() / 1e6
				if w.lastCommitTm > tms {
					// Time back
					w.lastCommitTm = tms
				}
				if w.lastCommitTm-tms > w.commitInterval &&
					w.wq.size() > 0 {
					// Do commit job
					if err = w.commitQueue(); nil != err {
						return
					}
				}
				time.Sleep(time.Millisecond * 20)
			}
		}
	}
}

func (w *worker) commitToExecutor(executor IJobExecutor, jobs []*WorkerEvent) error {
	retryTimes := 0
	maxRetryTimes := 0xfffffff
	retryInterval := time.Second * 2
	var err error

	for {
		if nil != err {
			// Retry and sleep
			atomic.StoreInt64(&w.status, WorkerStatusAbnormal)
			retryTimes++
			if retryTimes > maxRetryTimes {
				return errors.Trace(err)
			}
			logrus.Errorf("Commit queue error = %v", err)
			time.Sleep(retryInterval)
		}

		// Begin committing
		err = executor.Begin()
		if nil != err {
			continue
		}

		for _, job := range jobs {
			err = executor.Exec(job)
			if nil != err {
				break
			}
		}
		if nil != err {
			// We need rollback
			if rerr := executor.Rollback(); nil != rerr {
				logrus.Errorf("Rollback error = %v", rerr)
			}
			continue
		}
		err = executor.Commit()
		if nil != err {
			continue
		}

		// Success done
		break
	}

	atomic.StoreInt64(&w.status, WorkerStatusRunning)

	return nil
}

func (w *worker) commitToExecutors(jobs []*WorkerEvent) error {
	var err error

	for _, executor := range w.executors {
		if err = w.commitToExecutor(executor, jobs); nil != err {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *worker) commitQueue() error {
	jobs := w.wq.jobs()

	err := w.commitToExecutors(jobs)
	if nil != err {
		return errors.Trace(err)
	}
	// Reset jobs
	w.wq.reset()
	w.lastCommitTm = time.Now().UnixNano() / 1e6
	return nil
}
