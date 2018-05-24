package main

import (
	"context"
	"sync"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

const (
	minWorkerCount       = 1
	workerReportChanSize = 2560
	workerJobChanSize    = 2560
)

type workerReport struct {
	discardable bool
	wid         int
	err         error
}

type workerManager struct {
	workers []*worker
	wreport chan *workerReport
	done    context.Context
	stopFn  context.CancelFunc
	wg      sync.WaitGroup
	jobWg   sync.WaitGroup
}

func newWorkerManager(cfg *AppConfig) (*workerManager, error) {
	workerCount := cfg.WorkerCount
	if workerCount < minWorkerCount {
		logrus.Warnf("Minimal worker count is %d", minWorkerCount)
		workerCount = minWorkerCount
	}

	wm := &workerManager{
		wreport: make(chan *workerReport, workerReportChanSize),
		workers: make([]*worker, 0, workerCount),
	}

	// Create executor
	exet, err := executorFromDBConfig(cfg.Tos)
	if nil != err {
		return nil, errors.Trace(err)
	}

	for i := 0; i < workerCount; i++ {
		w := &worker{}
		w.wid = i
		w.executor = exet
		w.jobWg = &wm.jobWg
		wm.workers = append(wm.workers, w)
	}

	return wm, nil
}

func (w *workerManager) start() error {
	for _, wr := range w.workers {
		if err := wr.start(&w.wg, 0, 0); nil != err {
			return errors.Trace(err)
		}
	}

	w.wg.Add(1)
	go w.workerReportLoop()

	return nil
}

func (w *workerManager) stop() {
	w.jobWg.Wait()

	for _, v := range w.workers {
		v.stop()
	}
	// Once all worker is stop, worker report channel won't be used any more
	close(w.wreport)
	w.wg.Wait()
}

func (w *workerManager) uploadReport(r *workerReport) {
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

func (w *workerManager) workerReportLoop() {
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

func (w *workerManager) dispatchJob(job *execJob) error {
	index := 0
	w.workers[index].push(job)
	w.jobWg.Add(1)

	return nil
}
