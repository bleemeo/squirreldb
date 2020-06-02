package memberlist

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"squirreldb/types"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
)

const (
	countNodeForSeeding = 3
	// join are done periodically if the node is alone
	joinInterval = time.Minute
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[memberlist] ", log.LstdFlags)

type SeedProvider interface {
	SetAddress(clusterAddress string, apiAddress string) error
	Seeds() ([]string, error)
}

type Memberlist struct {
	SeedProvider     SeedProvider
	APIListenAddress string
	ClusterPort      int
	ClusterAddress   string

	memberlist   *memberlist.Memberlist
	lastFullJoin time.Time
}

// Run the Cassandra seed provider.
func (m *Memberlist) Run(ctx context.Context, readiness chan error) {
	err := m.init()

	readiness <- err

	if err != nil {
		return
	}

	ticker := time.NewTicker(joinInterval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			if m.memberlist.NumMembers() == 1 {
				err := m.join()
				if err != nil {
					logger.Printf("Unable to join cluster: %v", err)
				}
			}
		}
	}

	err = m.memberlist.Leave(15 * time.Second)
	if err != nil {
		logger.Printf("Unable to leave cluster: %v", err)
	}

	// Give short delay or it's likely that memberlist complain about "use of closed network connection"
	time.Sleep(100 * time.Millisecond)

	err = m.memberlist.Shutdown()
	if err != nil {
		logger.Printf("Unable to shutdown memberlist: %v", err)
	}
}

type node struct {
	apiAddress     string
	clusterAddress string
	isSelf         bool
	node           *memberlist.Node
}

// Nodes return list of known nodes of the cluster, in a consistent order.
func (m *Memberlist) Nodes() []types.Node {
	nodes := m.memberlist.Members()
	self := m.memberlist.LocalNode()
	results := make([]types.Node, len(nodes))

	for i, n := range nodes {
		apiAddr := fmt.Sprintf("%s:%s", n.Address(), string(n.Meta))
		clusterAddr := n.FullAddress().Addr

		results[i] = node{
			apiAddress:     apiAddr,
			clusterAddress: clusterAddr,
			isSelf:         n == self,
			node:           n,
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return strings.Compare(results[i].ClusterAddress(), results[j].ClusterAddress()) < 0
	})

	return results
}

func (n node) APIAddress() string {
	return n.apiAddress
}

func (n node) ClusterAddress() string {
	return n.clusterAddress
}

func (n node) IsSelf() bool {
	return n.isSelf
}

func (m *Memberlist) init() (err error) {
	m.lastFullJoin = time.Now()
	cfg := memberlist.DefaultLANConfig()

	cfg.Delegate = delegate{
		nodeMeta: []byte(m.APIListenAddress),
	}

	if m.ClusterPort != 0 {
		cfg.AdvertisePort = m.ClusterPort
		cfg.BindPort = m.ClusterPort
	}

	if m.ClusterAddress != "" {
		cfg.BindAddr = m.ClusterAddress
		cfg.AdvertiseAddr = m.ClusterAddress
	}

	cfg.Name = fmt.Sprintf("%s:%d", cfg.Name, cfg.BindPort)
	cfg.Logger = log.New(filteringLogger{}, "", 0)

	m.memberlist, err = memberlist.Create(cfg)
	if err != nil {
		return err
	}

	err = m.SeedProvider.SetAddress(m.memberlist.LocalNode().FullAddress().Addr, m.APIListenAddress)
	if err != nil {
		if err2 := m.memberlist.Shutdown(); err2 != nil {
			logger.Printf("Error during memberlist shutdown: %v", err)
		}

		return err
	}

	err = m.join()
	if err != nil {
		if err2 := m.memberlist.Shutdown(); err2 != nil {
			logger.Printf("Error during memberlist shutdown: %v", err)
		}

		return err
	}

	return nil
}

func (m *Memberlist) join() error {
	inCluster := m.memberlist.NumMembers() > 1

	seeds, err := m.SeedProvider.Seeds()
	if err != nil {
		return err
	}

	// We try to join the oldest countNodeForSeeding. If failing (none joined)
	// we move the more recent node until all seeds where tried.

	start := 0
	for start < len(seeds) {
		end := start + countNodeForSeeding
		if end > len(seeds) {
			end = len(seeds)
		}

		_, err := m.memberlist.Join(seeds[start:end])
		if err == nil {
			break
		}

		start = end
	}

	if m.memberlist.NumMembers() > 1 && !inCluster {
		members := m.memberlist.Members()
		memberStrings := make([]string, len(members))

		for i, n := range members {
			memberStrings[i] = n.FullAddress().Addr
		}

		logger.Printf("Joinned a SquirrelDB cluster. Current nodes are %v", memberStrings)
	}

	return nil
}
