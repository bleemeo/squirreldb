package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

// Client rovide a thin wrapper around Goredis client.
// This wrapper will auto-detect if the address point to a Redis cluster or
// a single instance client.
// It does this by assuming the Redis is a cluster, but if that fail AND
// single node query success then assume single instance.
// When either cluster or single node is assumed, we don't retry discovery.
type Client struct {
	Addresses []string

	l             sync.Mutex
	lastReload    time.Time
	singleClient  *goredis.Client
	clusterClient *goredis.ClusterClient
}

// Close dispatch Close to either cluster of single instance client.
// It will also reset the assumption of cluster vs single instance.
func (c *Client) Close() {
	c.l.Lock()
	defer c.l.Unlock()

	if c.clusterClient != nil {
		c.clusterClient.Close()
	}

	if c.singleClient != nil {
		c.singleClient.Close()
	}

	c.clusterClient = nil
	c.singleClient = nil
}

// Pipeline dispatch Pipeline to either cluster of single instance client.
func (c *Client) Pipeline(ctx context.Context) (goredis.Pipeliner, error) {
	if err := c.fixClient(ctx); err != nil {
		return nil, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.Pipeline(), nil
	}

	return c.singleClient.Pipeline(), nil
}

// Scan dispatch Scan to either cluster of single instance client.
func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) (*goredis.ScanCmd, error) {
	if err := c.fixClient(ctx); err != nil {
		return nil, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.Scan(ctx, cursor, match, count), nil
	}

	return c.singleClient.Scan(ctx, cursor, match, count), nil
}

// Publish dispatch Publish to either cluster of single instance client.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	if err := c.fixClient(ctx); err != nil {
		return 0, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.Publish(ctx, channel, message).Result()
	}

	return c.singleClient.Publish(ctx, channel, message).Result()
}

// Subscribe dispatch Subscribe to either cluster of single instance client.
func (c *Client) Subscribe(ctx context.Context, channel string) (*goredis.PubSub, error) {
	if err := c.fixClient(ctx); err != nil {
		return nil, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.Subscribe(ctx, channel), nil
	}

	return c.singleClient.Subscribe(ctx, channel), nil
}

// SAdd dispatch SAdd to either cluster of single instance client.
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	if err := c.fixClient(ctx); err != nil {
		return err
	}

	if c.clusterClient != nil {
		_, err := c.clusterClient.SAdd(ctx, key, members...).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}

		return nil
	}

	_, err := c.singleClient.SAdd(ctx, key, members...).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

// SRem dispatch SRem to either cluster of single instance client.
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	if err := c.fixClient(ctx); err != nil {
		return err
	}

	if c.clusterClient != nil {
		_, err := c.clusterClient.SRem(ctx, key, members...).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}

		return nil
	}

	_, err := c.singleClient.SRem(ctx, key, members...).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

// SPopN dispatch SPopN to either cluster of single instance client.
func (c *Client) SPopN(ctx context.Context, key string, count int64) ([]string, error) {
	if err := c.fixClient(ctx); err != nil {
		return nil, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.SPopN(ctx, key, count).Result()
	}

	return c.singleClient.SPopN(ctx, key, count).Result()
}

// SMembers dispatch SMembers to either cluster of single instance client.
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if err := c.fixClient(ctx); err != nil {
		return nil, err
	}

	if c.clusterClient != nil {
		return c.clusterClient.SMembers(ctx, key).Result()
	}

	return c.singleClient.SMembers(ctx, key).Result()
}

// IsCluster return whether the client is connected to a Redis cluster or not.
// This method will establish connection to Redis is not already done, and will
// always ping the Redis.
func (c *Client) IsCluster(ctx context.Context) (bool, error) {
	if err := c.fixClient(ctx); err != nil {
		return false, err
	}

	if c.clusterClient != nil {
		return true, c.clusterClient.Ping(ctx).Err()
	}

	return false, c.singleClient.Ping(ctx).Err()
}

// ShouldRetry return true when last command may be retried.
// when using redis cluster and a master is down, go-redis took quiet some time
// to discovery it.
// If shouldRetry return true, it means a refresh the cluster nodes state was
// submitted and last command may be retried.
// This always return false for single instance client.
func (c *Client) ShouldRetry(ctx context.Context) bool {
	c.l.Lock()
	defer c.l.Unlock()

	if c.clusterClient == nil {
		return false
	}

	if c.lastReload.IsZero() || time.Since(c.lastReload) > 10*time.Second {
		c.clusterClient.ReloadState(ctx)
		c.lastReload = time.Now()

		// ReloadState is asynchronious :(
		// Wait a bit in order for ReloadState to run
		time.Sleep(100 * time.Millisecond)

		return true
	}

	return false
}

func (c *Client) fixClient(ctx context.Context) error {
	c.l.Lock()
	defer c.l.Unlock()

	if c.singleClient == nil && c.clusterClient == nil {
		c.clusterClient = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs: c.Addresses,
		})

		if len(c.Addresses) == 1 {
			c.singleClient = goredis.NewClient(&goredis.Options{
				Addr: c.Addresses[0],
			})
		}
	}

	if c.singleClient == nil || c.clusterClient == nil {
		return nil
	}

	singleErr := c.singleClient.Ping(ctx).Err()
	clusterErr := c.clusterClient.Ping(ctx).Err()
	infoErr := c.singleClient.ClusterInfo(ctx).Err()

	switch {
	case clusterErr == nil:
		c.singleClient = nil
	case singleErr == nil && infoErr != nil:
		c.clusterClient = nil
	default:
		return fmt.Errorf("ping redis: %w", clusterErr)
	}

	return nil
}
