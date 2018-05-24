package main

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

const (
	defaultWorkerQueueSize           = 20
	defaultWorkerQueueCommitInterval = 200
)

const (
	workerStatusNone = iota
	workerStatusRunning
	workerStatusAbnormal
	workerStatusExit
)

type worker struct {
	wid            int
	wq             *workerQueue
	executor       IJobExecutor
	wg             *sync.WaitGroup
	jobWg          *sync.WaitGroup
	jobCh          chan *execJob
	lastCommitTm   int64
	commitInterval int64
	status         int64
}

func (w *worker) start(wg *sync.WaitGroup, wqsz, wqintv int) error {
	if nil == w.executor {
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
	w.jobCh = make(chan *execJob, workerJobChanSize)
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

func (w *worker) push(job *execJob) {
	w.jobCh <- job
}

func (w *worker) loop() {
	var err error

	defer func() {
		w.wg.Done()
		atomic.StoreInt64(&w.status, workerStatusExit)
	}()

	atomic.StoreInt64(&w.status, workerStatusRunning)

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

func (w *worker) commitQueue() error {
	jobs := w.wq.jobs()
	retryTimes := 0
	maxRetryTimes := 0xfffffff
	retryInterval := time.Second * 2
	var err error

	for {
		if nil != err {
			// Retry and sleep
			atomic.StoreInt64(&w.status, workerStatusAbnormal)
			retryTimes++
			if retryTimes > maxRetryTimes {
				return errors.Trace(err)
			}
			logrus.Errorf("Commit queue error = %v", err)
			time.Sleep(retryInterval)
		}

		// Begin committing
		err = w.executor.Begin()
		if nil != err {
			continue
		}

		for _, job := range jobs {
			err = w.executor.Exec(job)
			if nil != err {
				break
			}
		}
		if nil != err {
			// We need rollback
			if rerr := w.executor.Rollback(); nil != rerr {
				logrus.Errorf("Rollback error = %v", rerr)
			}
			continue
		}
		err = w.executor.Commit()
		if nil != err {
			continue
		}

		// Success done
		break
	}

	atomic.StoreInt64(&w.status, workerStatusRunning)
	// Reset jobs
	w.wq.reset()
	w.lastCommitTm = time.Now().UnixNano() / 1e6
	return nil
}
