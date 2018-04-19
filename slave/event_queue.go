package slave

import "github.com/sryanyuan/binp/binlog"

type eventQueue struct {
	eventCh chan *binlog.Event
	errorCh chan error
	lastErr error
}

func newEventQueue(bufferSize int) *eventQueue {
	return &eventQueue{
		eventCh: make(chan *binlog.Event, bufferSize),
		errorCh: make(chan error, 16),
	}
}
