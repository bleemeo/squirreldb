package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/redis/client"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// errors about topic name.
var (
	ErrTopicTooLong = errors.New("topic name is too long, maximum length is 255")
	ErrReadTooShort = errors.New("read is too short")
)

const (
	pubsubName              = "squirreldb:cluster:v1"
	clusterDiscoveryChannel = "discovery"

	discoveryMessagePeriod  = time.Minute
	discoveryStalenessDelay = 3*discoveryMessagePeriod + discoveryMessagePeriod/3 // 3 times the period + a margin
)

// Cluster implement types.Cluster using Redis pub/sub.
type Cluster struct {
	ID             uuid.UUID
	RedisOptions   config.Redis
	MetricRegistry prometheus.Registerer
	Keyspace       string
	Logger         zerolog.Logger

	client           *client.Client
	cancel           context.CancelFunc
	listeners        map[string][]func([]byte)
	redisChannel     string
	discoveryTSPerID map[string]int64 // map[uuid] -> millis
	l                sync.Mutex
	wg               sync.WaitGroup

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

	c.discoveryTSPerID = make(map[string]int64)

	c.redisChannel = c.Keyspace + pubsubName
	c.client = client.New(c.RedisOptions)

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

	c.Subscribe(clusterDiscoveryChannel, c.handleDiscoveryMessage)

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

	if c.listeners == nil {
		c.listeners = make(map[string][]func([]byte))
	}

	c.listeners[topic] = append(c.listeners[topic], callback)
}

func (c *Cluster) run(ctx context.Context, pubsub *goredis.PubSub) {
	defer c.wg.Done()

	ch := pubsub.Channel()
	discoveryTicker := time.NewTicker(discoveryMessagePeriod)

	defer discoveryTicker.Stop()

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

			for _, f := range c.listeners[topic] {
				f(message)
			}

			c.metrics.MessageSeconds.WithLabelValues("receive").Observe(time.Since(start).Seconds())
		case <-discoveryTicker.C:
			payload, err := makeDiscoveryPayload(c.ID.String())
			if err != nil {
				c.Logger.Err(err).Msg("failed to encode discovery payload")
			} else {
				err = c.Publish(ctx, clusterDiscoveryChannel, payload)
				if err != nil {
					c.Logger.Err(err).Msg("failed to publish cluster discovery message")
				}
			}
		case <-ctx.Done():
			continue
		}
	}

	pubsub.Close()
}

func (c *Cluster) Size() int {
	c.l.Lock()
	defer c.l.Unlock()

	now := time.Now().UnixMilli()
	purgeDelay := discoveryStalenessDelay.Milliseconds()
	aliveCount := 1 // We also count *this* SquirrelDB

	for id, ts := range c.discoveryTSPerID {
		if now-ts > purgeDelay {
			delete(c.discoveryTSPerID, id)
		} else {
			aliveCount++
		}
	}

	return aliveCount
}

func (c *Cluster) handleDiscoveryMessage(msg []byte) {
	var payload discoveryPayload

	dec := gob.NewDecoder(bytes.NewReader(msg))

	err := dec.Decode(&payload)
	if err != nil {
		c.Logger.Err(err).Msg("failed to decode discovery message")

		return
	}

	if payload.SenderID == c.ID.String() {
		return // We don't store our own messages
	}

	c.l.Lock()
	defer c.l.Unlock()

	c.discoveryTSPerID[payload.SenderID] = payload.TimestampMs
}

type discoveryPayload struct {
	SenderID    string
	TimestampMs int64
}

func makeDiscoveryPayload(id string) ([]byte, error) {
	payload := discoveryPayload{
		SenderID:    id,
		TimestampMs: time.Now().UnixMilli(),
	}

	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)

	err := enc.Encode(payload)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func encode(topic string, message []byte) (string, error) {
	result := bytes.NewBuffer(make([]byte, len(message))[:0])

	b64w := base64.NewEncoder(base64.StdEncoding, result)
	snapWriter := snappy.NewBufferedWriter(b64w)

	if err := binary.Write(snapWriter, binary.BigEndian, uint8(len(topic))); err != nil { //nolint:gosec
		return "", err
	}

	if err := binary.Write(snapWriter, binary.BigEndian, uint32(len(message))); err != nil { //nolint:gosec
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
