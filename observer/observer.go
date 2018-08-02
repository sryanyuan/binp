package observer

import (
	"sync"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/binlog"
)

// Observer can observe binlog event
type Observer interface {
	OnEvent(*binlog.Event) error
	GetID() int
	SetID(int)
}

// BaseObserver implement the Observer GetID and SetID method
type BaseObserver struct {
	id int
}

// NotifyChain holds observers and notify them if event happened
type NotifyChain struct {
	mu  sync.Mutex
	oid int
	obs []Observer
}

// AddObserver add a observer to the chain
func (c *NotifyChain) AddObserver(ob Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.oid++
	ob.SetID(c.oid)
	if nil == c.obs {
		c.obs = make([]Observer, 0, 8)
	}
	for i, v := range c.obs {
		if nil == v {
			c.obs[i] = ob
			return
		}
	}
	c.obs = append(c.obs, ob)
}

// RemoveObserver remove a observer from the chain
func (c *NotifyChain) RemoveObserver(ob Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, v := range c.obs {
		if nil == v {
			continue
		}
		if v.GetID() == ob.GetID() {
			c.obs[i] = nil
			return
		}
	}
}

// Broadcast notify all observer event occurs
func (c *NotifyChain) Broadcast(evt *binlog.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, v := range c.obs {
		if nil == v {
			continue
		}
		if err := v.OnEvent(evt); nil != err {
			return errors.Trace(err)
		}
	}
	return nil
}
