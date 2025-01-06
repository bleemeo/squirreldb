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

package connection

import (
	"sync"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type connectObserver struct {
	l                 sync.Mutex
	lastObservedError connectError
	logger            zerolog.Logger
	wakeRunLoop       chan interface{}
}

type connectError struct {
	err         error
	hostAndPort string
}

func (obs *connectObserver) GetAndClearLastObservation() connectError {
	obs.l.Lock()
	defer obs.l.Unlock()

	value := obs.lastObservedError
	obs.lastObservedError = connectError{}

	return value
}

func (obs *connectObserver) ObserveConnect(msg gocql.ObservedConnect) {
	if msg.Err != nil {
		obs.l.Lock()
		obs.lastObservedError = connectError{
			err:         msg.Err,
			hostAndPort: msg.Host.HostnameAndPort(),
		}
		obs.l.Unlock()

		select {
		case obs.wakeRunLoop <- nil:
		default:
		}

		obs.logger.Debug().
			Err(msg.Err).
			Str("HostnameAndPort", msg.Host.HostnameAndPort()).
			Msg("ObserveConnect see an error")
	}
}
