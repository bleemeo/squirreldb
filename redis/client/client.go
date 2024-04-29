package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/config"

	goredis "github.com/redis/go-redis/v9"
)

var errNotPem = errors.New("not a PEM file")

// Client provides a thin wrapper around Goredis client.
// This wrapper will auto-detect if the address point to a Redis cluster or
// a single instance client.
// It does this by assuming the Redis is a cluster, but if that fail AND
// single node query success then assume single instance.
// When either cluster or single node is assumed, we don't retry discovery.
type Client struct {
	opts config.Redis

	lastReload    time.Time
	singleClient  *goredis.Client
	clusterClient *goredis.ClusterClient
	l             sync.Mutex
}

// New returns a redis client.
func New(opts config.Redis) *Client {
	return &Client{opts: opts}
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

// ForEachMaster dispatch ForEachMaster to either all master of the cluster or single instance client.
func (c *Client) ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *goredis.Client) error) error {
	if err := c.fixClient(ctx); err != nil {
		return err
	}

	if c.clusterClient != nil {
		return c.clusterClient.ForEachMaster(ctx, fn)
	}

	return fn(ctx, c.singleClient)
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

	tlsConfig, err := c.tlsConfig()
	if err != nil {
		return err
	}

	if c.singleClient == nil && c.clusterClient == nil {
		c.clusterClient = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:     c.opts.Addresses,
			Username:  c.opts.Username,
			Password:  c.opts.Password,
			TLSConfig: tlsConfig,
		})

		if len(c.opts.Addresses) == 1 {
			c.singleClient = goredis.NewClient(&goredis.Options{
				Addr:      c.opts.Addresses[0],
				Username:  c.opts.Username,
				Password:  c.opts.Password,
				TLSConfig: tlsConfig,
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
		return fmt.Errorf("ping redis: single node client: %w, cluster client: %w", singleErr, clusterErr)
	}

	return nil
}

func (c *Client) tlsConfig() (*tls.Config, error) {
	if !c.opts.SSL {
		return nil, nil //nolint:nilnil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.opts.SSLInsecure, //nolint:gosec
	}

	if c.opts.CAPath != "" {
		rootCAs, err := loadRootCAs(c.opts.CAPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load CA: %w", err)
		}

		tlsConfig.RootCAs = rootCAs
	}

	if c.opts.CertPath != "" || c.opts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(c.opts.CertPath, c.opts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load X509 key pair: %w", err)
		}

		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	return tlsConfig, nil
}

func loadRootCAs(caFile string) (*x509.CertPool, error) {
	rootCAs := x509.NewCertPool()

	certs, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	ok := rootCAs.AppendCertsFromPEM(certs)
	if !ok {
		return nil, errNotPem
	}

	return rootCAs, nil
}
