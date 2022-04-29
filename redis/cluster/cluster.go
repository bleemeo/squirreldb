package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"squirreldb/logger"
	"squirreldb/redis/client"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// errors about topic name.
var (
	ErrTopicTooLong = errors.New("topic name is too long, maximum length is 255")
	ErrReadTooShort = errors.New("read is too short")
)

const (
	pubsubName = "squirreldb:cluster:v1"
)

// Cluster implement types.Cluster using Redis pub/sub.
type Cluster struct {
	Addresses      []string
	MetricRegistry prometheus.Registerer
	Keyspace       string
	Logger         zerolog.Logger

	client       *client.Client
	cancel       context.CancelFunc
	listenner    map[string][]func([]byte)
	redisChannel string
	l            sync.Mutex
	wg           sync.WaitGroup

	metrics *metrics
}

func (c *Cluster) Start(ctx context.Context) error {
	if c.cancel != nil {
		return nil
	}

	if c.metrics == nil {
		reg := c.MetricRegistry
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}

		c.metrics = newMetrics(reg)
	}

	c.redisChannel = c.Keyspace + pubsubName
	c.client = &client.Client{
		Addresses: c.Addresses,
	}

	cluster, err := c.client.IsCluster(ctx)
	if err != nil {
		c.client.Close()

		return fmt.Errorf("cluster-redis failed to connect to redis: %w", err)
	}

	if cluster {
		c.Logger.Info().Msg("detected cluster")
	} else {
		c.Logger.Info().Msg("detected single")
	}

	pubsub, err := c.client.Subscribe(ctx, c.redisChannel)
	if err != nil {
		c.client.Close()

		return err
	}

	_, err = pubsub.Receive(ctx)
	if err != nil {
		c.client.Close()

		return err
	}

	ctx, cancel := context.WithCancel(context.Background()) //nolint:contextcheck
	c.cancel = cancel

	c.wg.Add(1)

	go func() {
		defer logger.ProcessPanic()

		c.run(ctx, pubsub)
	}()

	return nil
}

func (c *Cluster) Close() error {
	return c.Stop()
}

func (c *Cluster) Stop() error {
	if c.cancel == nil {
		return errors.New("not started")
	}

	c.cancel()
	c.cancel = nil
	c.wg.Wait()

	c.client.Close()

	return nil
}

// Publish send a message to given topic. Publish must not be called before Start() (or after Stop()).
func (c *Cluster) Publish(ctx context.Context, topic string, message []byte) error {
	start := time.Now()

	defer func() {
		c.metrics.MessageSeconds.WithLabelValues("sent").Observe(time.Since(start).Seconds())
	}()

	if len(topic) > 255 {
		return ErrTopicTooLong
	}

	payload, err := encode(topic, message)
	if err != nil {
		return err
	}

	c.l.Lock()
	defer c.l.Unlock()

	_, err = c.client.Publish(ctx, c.redisChannel, payload)

	return err
}

// Subscribe to message on given topic. The callback should be quick or it may cause message lost.
func (c *Cluster) Subscribe(topic string, callback func([]byte)) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.listenner == nil {
		c.listenner = make(map[string][]func([]byte))
	}

	c.listenner[topic] = append(c.listenner[topic], callback)
}

func (c *Cluster) run(ctx context.Context, pubsub *goredis.PubSub) {
	defer c.wg.Done()

	ch := pubsub.Channel()

	for ctx.Err() == nil {
		select {
		case msg := <-ch:
			start := time.Now()

			topic, message, err := decode(msg.Payload)
			if err != nil {
				c.Logger.Err(err).Msg("failed to decode message")
				c.metrics.MessageSeconds.WithLabelValues("receive").Observe(time.Since(start).Seconds())

				continue
			}

			for _, f := range c.listenner[topic] {
				f(message)
			}

			c.metrics.MessageSeconds.WithLabelValues("receive").Observe(time.Since(start).Seconds())
		case <-ctx.Done():
			continue
		}
	}

	pubsub.Close()
}

func encode(topic string, message []byte) (string, error) {
	result := bytes.NewBuffer(make([]byte, len(message))[:0])

	b64w := base64.NewEncoder(base64.StdEncoding, result)
	snapWriter := snappy.NewBufferedWriter(b64w)

	if err := binary.Write(snapWriter, binary.BigEndian, uint8(len(topic))); err != nil {
		return "", err
	}

	if err := binary.Write(snapWriter, binary.BigEndian, uint32(len(message))); err != nil {
		return "", err
	}

	if _, err := snapWriter.Write([]byte(topic)); err != nil {
		return "", err
	}

	if _, err := snapWriter.Write(message); err != nil {
		return "", err
	}

	snapWriter.Close()
	b64w.Close()

	return result.String(), nil
}

func decode(input string) (topic string, message []byte, err error) {
	b64r := base64.NewDecoder(base64.StdEncoding, bytes.NewReader([]byte(input)))
	snapReader := snappy.NewReader(b64r)

	var (
		topicLen   uint8
		messageLen uint32
	)

	if err := binary.Read(snapReader, binary.BigEndian, &topicLen); err != nil {
		return "", nil, err
	}

	if err := binary.Read(snapReader, binary.BigEndian, &messageLen); err != nil {
		return "", nil, err
	}

	buffer := make([]byte, topicLen)

	if topicLen > 0 {
		if n, err := snapReader.Read(buffer); err != nil {
			return "", nil, err
		} else if n != int(topicLen) {
			return "", nil, fmt.Errorf("%w: read %d, want %d", ErrReadTooShort, n, topicLen)
		}
	}

	message = make([]byte, messageLen)

	if messageLen > 0 {
		if n, err := io.ReadFull(snapReader, message); err != nil {
			return "", nil, err
		} else if n != int(messageLen) {
			return "", nil, fmt.Errorf("%w: read %d, want %d", ErrReadTooShort, n, messageLen)
		}
	}

	return string(buffer), message, nil
}
