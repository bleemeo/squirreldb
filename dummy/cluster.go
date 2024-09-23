package dummy

import (
	"context"
	"sync"
)

// LocalCluster implement types.Cluster but only for clients sharing the same LocalCluster object.
type LocalCluster struct {
	listeners map[string][]func([]byte)
	l         sync.Mutex
}

func (c *LocalCluster) Publish(_ context.Context, topic string, message []byte) error {
	c.l.Lock()
	defer c.l.Unlock()

	for _, f := range c.listeners[topic] {
		f(message)
	}

	return nil
}

func (c *LocalCluster) Subscribe(topic string, callback func([]byte)) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.listeners == nil {
		c.listeners = make(map[string][]func([]byte))
	}

	c.listeners[topic] = append(c.listeners[topic], callback)
}

func (c *LocalCluster) Size() int {
	return 1
}

func (c *LocalCluster) Close() error {
	c.l.Lock()
	defer c.l.Unlock()

	c.listeners = nil

	return nil
}
