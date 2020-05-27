package wal

import (
	"bytes"
	"encoding/gob"
	"io"
	"squirreldb/types"
	"sync"
	"sync/atomic"
)

type encoder struct {
	buffer  bytes.Buffer
	encoder *gob.Encoder
}

func decode(src []byte) (dst []types.MetricData, err error) {
	r := bytes.NewReader(src)
	dec := gob.NewDecoder(r)

	for {
		var tmp []types.MetricData

		err = dec.Decode(&tmp)
		if err != nil {
			break
		}

		dst = append(dst, tmp...)
	}

	if err == io.EOF {
		err = nil
	}

	return dst, err
}

func (c *encoder) Encode(src []types.MetricData) error {
	return c.encoder.Encode(src)
}

func (c *encoder) Bytes() []byte {
	return c.buffer.Bytes()
}

func (c *encoder) Reset() {
	c.buffer.Reset()
	c.encoder = gob.NewEncoder(&c.buffer)
}

type pool struct {
	pool              chan *encoder
	poolSize          int
	byteSize          int32
	gobDefinitionSize int
	mutex             sync.Mutex
}

func (p *pool) Init(size int) {
	p.poolSize = size
	p.pool = make(chan *encoder, p.poolSize)

	for n := 0; n < p.poolSize; n++ {
		e := &encoder{}
		e.Reset()
		p.pool <- e
	}

	// We can't concatennate two Gob stream, because we can't have twice the
	// same type definition.
	// Find the size of the type definition (we only have one type, []types.MetricData)
	e := &encoder{}
	e.Reset()

	err := e.Encode([]types.MetricData{})
	if err != nil {
		logger.Fatalf("unexpected error: %v", err)
	}

	oneLength := e.buffer.Len()

	err = e.Encode([]types.MetricData{})
	if err != nil {
		logger.Fatalf("unexpected error: %v", err)
	}

	emptyEntrySize := e.buffer.Len() - oneLength
	p.gobDefinitionSize = oneLength - emptyEntrySize
}

func (p *pool) Encode(src []types.MetricData) error {
	_, _, err := p.EncodeEx(src)
	return err
}

func (p *pool) EncodeEx(src []types.MetricData) (newSize int, addedBytes int, err error) {
	e := <-p.pool
	previousSize := e.buffer.Len()
	err = e.Encode(src)
	addedBytes = e.buffer.Len() - previousSize
	tmp := atomic.AddInt32(&p.byteSize, int32(addedBytes))

	p.pool <- e

	return int(tmp), addedBytes, err
}

func (p *pool) Len() int {
	tmp := atomic.LoadInt32(&p.byteSize)
	return int(tmp)
}

// BytesAndReset will return the current Bytes() of the encoder and reset it
func (p *pool) BytesAndReset() []byte {
	p.mutex.Lock()

	var blocked []*encoder

	for n := 0; n < p.poolSize; n++ {
		blocked = append(blocked, <-p.pool)
	}

	buffer := make([]byte, 0, int(p.byteSize))

	for _, e := range blocked {
		if e.buffer.Len() == 0 {
			continue
		}

		if len(buffer) == 0 {
			// Keep the Gob header from the first encoder
			buffer = append(buffer, e.Bytes()...)
		} else {
			// but drop it from other
			buffer = append(buffer, e.Bytes()[p.gobDefinitionSize:]...)
		}
	}

	p.byteSize = 0

	p.mutex.Unlock()

	for _, e := range blocked {
		e.Reset()

		p.pool <- e
	}

	return buffer
}
