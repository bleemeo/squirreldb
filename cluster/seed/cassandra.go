package seed

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"squirreldb/debug"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const (
	updateTime = 1 * time.Minute
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[seed] ", log.LstdFlags)

// Cassandra provide seed from Cassandra.
type Cassandra struct {
	Session        *gocql.Session
	SchemaLock     sync.Locker
	clusterAddress string
	apiAddress     string
	addressUpdate  chan addressUpdate
	updateDone     chan error
	createdAt      time.Time
}

type addressUpdate struct {
	apiAddress     string
	clusterAddress string
}

// Run the Cassandra seed provider.
func (c *Cassandra) Run(ctx context.Context, readiness chan error) {
	err := c.init()
	if err != nil {
		readiness <- err
		return
	}

	readiness <- nil

	ticker := time.NewTicker(updateTime)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case new := <-c.addressUpdate:
			if c.clusterAddress != new.clusterAddress {
				c.remove()
			}

			c.clusterAddress = new.clusterAddress
			c.apiAddress = new.apiAddress
			c.updateDone <- c.update()
		case <-ticker.C:
			_ = c.update()
		}
	}

	c.remove()
}

// SetAddress change the node address in the Cassandra tables.
func (c *Cassandra) SetAddress(clusterAddress string, apiAddress string) error {
	c.addressUpdate <- addressUpdate{
		apiAddress:     apiAddress,
		clusterAddress: clusterAddress,
	}

	return <-c.updateDone
}

// Seeds return all known clusterAddress, sorted by age.
//
// It do NOT return self in the list.
func (c *Cassandra) Seeds() ([]string, error) {
	iter := c.Session.Query("SELECT cluster_address, created_at FROM cluster_seed").Iter()

	type entry struct {
		address   string
		createdAt time.Time
	}

	var (
		address   string
		createdAt time.Time
		results   []entry
	)

	for iter.Scan(&address, &createdAt) {
		if address == c.clusterAddress {
			continue
		}

		results = append(results, entry{
			address:   address,
			createdAt: createdAt,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].createdAt.Before(results[j].createdAt)
	})

	addresses := make([]string, len(results))

	for i, ent := range results {
		addresses[i] = ent.address
	}

	return addresses, nil
}

func (c *Cassandra) init() error {
	c.SchemaLock.Lock()
	defer c.SchemaLock.Unlock()

	c.createdAt = time.Now()

	query := `CREATE TABLE IF NOT EXISTS cluster_seed (
			cluster_address ascii,
			api_address ascii,
			created_at timestamp,
			last_update timestamp,
			PRIMARY KEY (cluster_address)
		)`

	if err := c.Session.Query(query).Consistency(gocql.All).Exec(); err != nil {
		return err
	}

	c.addressUpdate = make(chan addressUpdate)
	c.updateDone = make(chan error)

	return nil
}

func (c *Cassandra) update() error {
	if c.clusterAddress == "" {
		return fmt.Errorf("invalid empty value for clusterAddress")
	}

	err := c.Session.Query(
		"INSERT INTO cluster_seed (cluster_address, api_address, created_at, last_update) VALUES (?, ?, ?, ?) USING TTL ?",
		c.clusterAddress,
		c.apiAddress,
		c.createdAt,
		time.Now(),
		int((updateTime * 5).Seconds()),
	).Exec()

	if err != nil {
		debug.Print(1, logger, "Failed to update seed: %v", err)
		return err
	}

	return nil
}

func (c *Cassandra) remove() {
	if c.clusterAddress == "" {
		return
	}

	err := c.Session.Query(
		"DELETE FROM cluster_seed WHERE cluster_address = ?",
		c.clusterAddress,
	).Exec()

	if err != nil {
		logger.Printf("Failed to delete seed: %v", err)
	}
}
