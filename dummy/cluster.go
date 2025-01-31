// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
