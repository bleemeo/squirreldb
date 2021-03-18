package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"squirreldb/redis/client"
	"sync"

	goredis "github.com/go-redis/redis/v8"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[redis-cluster] ", log.LstdFlags)

var (
	ErrTopicTooLong = errors.New("topic name is too long, maximum length is 255")
	ErrReadTooShort = errors.New("read is too short")
)

const (
	prefixLength = 8
	pubsubName   = "squirreldb:cluster:v1"
)

// Cluster implement types.Cluster using Redis pub/sub.
type Cluster struct {
	Addresses        []string
	MetricRegistry   prometheus.Registerer
	ChannelNamespace string
	l                sync.Mutex
	prefix           []byte
	client           *client.Client
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	listenner        map[string][]func([]byte)
	redisChannel     string

	messageReceived prometheus.Counter
}

func (c *Cluster) Start(ctx context.Context) error {
	if c.cancel != nil {
		return nil
	}

	if err := c.makeMetrics(); err != nil {
		return err
	}

	c.redisChannel = c.ChannelNamespace + pubsubName
	c.prefix = makePrefix()
	c.client = &client.Client{
		Addresses: c.Addresses,
	}

	cluster, err := c.client.IsCluster(ctx)
	if err != nil {
		c.client.Close()

		return err
	}

	if cluster {
		logger.Println("detected cluster")
	} else {
		logger.Println("detected single")
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

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)

	go c.run(ctx, pubsub)

	return nil
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
	if len(topic) > 255 {
		return ErrTopicTooLong
	}

	payload, err := encode(topic, c.prefix, message)
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

func (c *Cluster) makeMetrics() error {
	c.messageReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "redis_cluster",
		Name:      "message_total",
		Help:      "Total messages received by Redis (from myself or not)",
	})

	if c.MetricRegistry != nil {
		if err := c.MetricRegistry.Register(c.messageReceived); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) run(ctx context.Context, pubsub *goredis.PubSub) {
	defer c.wg.Done()

	ch := pubsub.Channel()

	for ctx.Err() == nil {
		select {
		case msg := <-ch:
			c.messageReceived.Inc()

			topic, sender, message, err := decode(msg.Payload)
			if err != nil {
				logger.Printf("failed to decode message: %v", err)
				continue
			}

			if bytes.Equal(sender, c.prefix) {
				continue
			}

			for _, f := range c.listenner[topic] {
				f(message)
			}
		case <-ctx.Done():
			continue
		}
	}

	pubsub.Close()
}

func makePrefix() []byte {
	prefix := make([]byte, prefixLength)

	_, err := rand.Read(prefix) // nolint: gosec
	if err != nil {
		panic(err)
	}

	return prefix
}

func encode(topic string, sender []byte, message []byte) (string, error) {
	result := bytes.NewBuffer(make([]byte, len(message))[:0])

	b64w := base64.NewEncoder(base64.StdEncoding, result)
	snapWriter := snappy.NewBufferedWriter(b64w)

	if err := binary.Write(snapWriter, binary.BigEndian, uint8(len(topic))); err != nil {
		return "", err
	}

	if err := binary.Write(snapWriter, binary.BigEndian, uint32(len(message))); err != nil {
		return "", err
	}

	if _, err := snapWriter.Write(sender); err != nil {
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

func decode(input string) (topic string, sender []byte, message []byte, err error) {
	b64r := base64.NewDecoder(base64.StdEncoding, bytes.NewReader([]byte(input)))
	snapReader := snappy.NewReader(b64r)

	var (
		topicLen   uint8
		messageLen uint32
	)

	if err := binary.Read(snapReader, binary.BigEndian, &topicLen); err != nil {
		return "", nil, nil, err
	}

	if err := binary.Read(snapReader, binary.BigEndian, &messageLen); err != nil {
		return "", nil, nil, err
	}

	sender = make([]byte, prefixLength)

	if n, err := snapReader.Read(sender); err != nil {
		return "", nil, nil, err
	} else if n != prefixLength {
		return "", nil, nil, fmt.Errorf("%w: read %d, want %d", ErrReadTooShort, n, prefixLength)
	}

	buffer := make([]byte, topicLen)

	if topicLen > 0 {
		if n, err := snapReader.Read(buffer); err != nil {
			return "", nil, nil, err
		} else if n != int(topicLen) {
			return "", nil, nil, fmt.Errorf("%w: read %d, want %d", ErrReadTooShort, n, topicLen)
		}
	}

	message = make([]byte, messageLen)

	if messageLen > 0 {
		if n, err := io.ReadFull(snapReader, message); err != nil {
			return "", nil, nil, err
		} else if n != int(messageLen) {
			return "", nil, nil, fmt.Errorf("%w: read %d, want %d", ErrReadTooShort, n, messageLen)
		}
	}

	return string(buffer), sender, message, nil
}
