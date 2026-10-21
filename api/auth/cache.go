// Copyright 2015-2026 Bleemeo
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

package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"sync"
	"time"
)

const (
	verifyCacheTTL        = time.Minute
	verifyCacheMaxEntries = 1024
)

// verifyCache remembers the successful basic auth verifications for a short time,
// so a client sending a request every few seconds doesn't pay the bcrypt cost on
// each request. Entries are keyed by a salted sha256 of the username and password:
// the cache never holds a password in clear text.
type verifyCache struct {
	now  func() time.Time
	salt [32]byte

	l       sync.Mutex
	entries map[[sha256.Size]byte]time.Time
}

func newVerifyCache() *verifyCache {
	c := &verifyCache{
		now:     time.Now,
		entries: make(map[[sha256.Size]byte]time.Time),
	}

	_, _ = rand.Read(c.salt[:])

	return c
}

func (c *verifyCache) key(username, password string) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(c.salt[:])
	_, _ = h.Write([]byte(username))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(password))

	var key [sha256.Size]byte

	h.Sum(key[:0])

	return key
}

// Has returns whether this username and password were successfully verified recently.
func (c *verifyCache) Has(username, password string) bool {
	key := c.key(username, password)

	c.l.Lock()
	defer c.l.Unlock()

	expiration, ok := c.entries[key]
	if !ok {
		return false
	}

	if c.now().After(expiration) {
		delete(c.entries, key)

		return false
	}

	return true
}

// Add remembers a successful verification.
func (c *verifyCache) Add(username, password string) {
	key := c.key(username, password)

	c.l.Lock()
	defer c.l.Unlock()

	now := c.now()

	if len(c.entries) >= verifyCacheMaxEntries {
		for k, expiration := range c.entries {
			if now.After(expiration) {
				delete(c.entries, k)
			}
		}

		// Only valid credentials are cached, so this only happens with a lot of
		// users. Starting over just costs one bcrypt verification per user.
		if len(c.entries) >= verifyCacheMaxEntries {
			clear(c.entries)
		}
	}

	c.entries[key] = now.Add(verifyCacheTTL)
}
