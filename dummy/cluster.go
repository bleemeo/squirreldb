package dummy

import (
	"context"
	"sync"
)

// LocalCluster implement types.Cluster but only for client sharing the same LocalCluster object.
type LocalCluster struct {
	listenner map[string][]func([]byte)
	l         sync.Mutex
}

func (c *LocalCluster) Publish(ctx context.Context, topic string, message []byte) error {
	c.l.Lock()
	defer c.l.Unlock()

	for _, f := range c.listenner[topic] {
		f(message)
	}

	return nil
}

func (c *LocalCluster) Subscribe(topic string, callback func([]byte)) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.listenner == nil {
		c.listenner = make(map[string][]func([]byte))
	}

	c.listenner[topic] = append(c.listenner[topic], callback)
}

func (c *LocalCluster) Close() error {
	c.l.Lock()
	defer c.l.Unlock()

	c.listenner = nil

	return nil
}
