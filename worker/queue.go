package worker

// Not thread safe
type workerQueue struct {
	buf  []*WorkerEvent
	capa int
	sz   int
}

func newWorkerQueue(sz int) *workerQueue {
	q := &workerQueue{}
	q.buf = make([]*WorkerEvent, 0, sz)
	q.sz = 0
	q.capa = sz
	return q
}

func (q *workerQueue) push(job *WorkerEvent) {
	if q.sz >= q.capa {
		panic("queue already full")
	}
	q.buf = append(q.buf, job)
	q.sz++
}

func (q *workerQueue) full() bool {
	return q.sz == q.capa
}

func (q *workerQueue) size() int {
	return q.sz
}

func (q *workerQueue) reset() {
	q.sz = 0
	q.buf = q.buf[0:0]
}

func (q *workerQueue) jobs() []*WorkerEvent {
	return q.buf
}
