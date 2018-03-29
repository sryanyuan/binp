package anumber

import "sync/atomic"

// AtomicInt32 is a atomic int32 type
type AtomicInt32 int32

// Get returns the value atomically
func (i *AtomicInt32) Get() int32 {
	return atomic.LoadInt32((*int32)(i))
}

// Set stores the value atomically
func (i *AtomicInt32) Set(v int32) {
	atomic.StoreInt32((*int32)(i), v)
}

// AtomicInt64 is a atomic int64 type
type AtomicInt64 int64

// Get returns the value atomically
func (i *AtomicInt64) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

// Set stores the value atomically
func (i *AtomicInt64) Set(v int64) {
	atomic.StoreInt64((*int64)(i), v)
}
