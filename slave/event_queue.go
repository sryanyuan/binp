package slave

import (
	"github.com/sryanyuan/binp/bevent"
)

type eventQueue struct {
	eventCh chan *bevent.BinlogEvent
	errorCh chan error
	lastErr error
}

func newEventQueue(bufferSize int) *eventQueue {
	return &eventQueue{
		eventCh: make(chan *bevent.BinlogEvent, bufferSize),
		errorCh: make(chan error, 16),
	}
}
