package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"squirreldb/types"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

const (
	countNodeForSeeding = 3
	// join are done periodically if the node is alone
	joinInterval      = time.Minute
	concurrentRequest = 10
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[cluster] ", log.LstdFlags)

type SeedProvider interface {
	SetAddress(clusterAddress string, apiAddress string) error
	Seeds() ([]string, error)
}

type Cluster struct {
	SeedProvider     SeedProvider
	APIListenAddress string
	ClusterPort      int
	ClusterAddress   string

	mutex          sync.Mutex
	sentRPC        map[uint32]rpcRequest
	nextRequestID  uint32
	memberlist     *memberlist.Memberlist
	notifyChan     chan []byte
	closing        bool
	wg             sync.WaitGroup
	requestToken   chan interface{}
	requestHandler func(uint8, []byte) ([]byte, error)
}

type rpcRequest struct {
	sentTime   time.Time
	replyChan  chan []byte
	replyError chan error
}

// Run the Cassandra seed provider.
func (c *Cluster) Run(ctx context.Context, readiness chan error) {
	err := c.init()

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
			num := c.memberlist.NumMembers()
			numberNodes.Set(float64(num))

			if num == 1 {
				err := c.join()
				if err != nil {
					logger.Printf("Unable to join cluster: %v", err)
				}
			}
		}
	}

	err = c.memberlist.Leave(15 * time.Second)
	if err != nil {
		logger.Printf("Unable to leave cluster: %v", err)
	}

	c.mutex.Lock()
	c.closing = true
	c.mutex.Unlock()

	// Give short delay or it's likely that memberlist complain about "use of closed network connection"
	time.Sleep(100 * time.Millisecond)

	err = c.memberlist.Shutdown()
	if err != nil {
		logger.Printf("Unable to shutdown memberlist: %v", err)
	}

	numberNodes.Set(0)

	// Give short delay because Shutdown return *before* memberlist really stopped :(
	time.Sleep(100 * time.Millisecond)

	// Wait for completion of every pending request.
	for n := 0; n < concurrentRequest; n++ {
		<-c.requestToken
	}
	close(c.notifyChan)
	c.wg.Wait()
}

type node struct {
	apiAddress     string
	clusterAddress string
	isSelf         bool
	node           *memberlist.Node
}

// Nodes return list of known nodes of the cluster, in a consistent order.
func (c *Cluster) Nodes() []types.Node {
	start := time.Now()

	defer func() {
		requestsSecondsNode.Observe(time.Since(start).Seconds())
	}()

	nodes := c.memberlist.Members()
	self := c.memberlist.LocalNode()
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

func (c *Cluster) Send(n types.Node, requestType uint8, data []byte) ([]byte, error) {
	return c.sendUsingCluster(n, requestType, data)
}

func (c *Cluster) SetRequestHandler(f func(requestType uint8, data []byte) ([]byte, error)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.requestHandler = f
}

// sendUsingCluster send message (and wait reply using cluster messaging).
func (c *Cluster) sendUsingCluster(tmp types.Node, requestType uint8, data []byte) ([]byte, error) {
	start := time.Now()

	n, ok := tmp.(node)
	if !ok {
		return nil, errors.New("node is not a object from cluster package. Can't use it")
	}

	replyChan := make(chan []byte)
	replyError := make(chan error)

	c.mutex.Lock()
	id := c.nextRequestID
	c.nextRequestID++
	c.sentRPC[id] = rpcRequest{
		sentTime:   time.Now(),
		replyChan:  replyChan,
		replyError: replyError,
	}
	c.mutex.Unlock()

	mySelf := c.memberlist.LocalNode()
	packet := rpcPacket{
		PacketType:     msgRequest,
		RequestID:      id,
		ReplyToAddress: mySelf.FullAddress().Addr,
		RequestType:    requestType,
		Payload:        data,
	}

	var (
		reply []byte
	)

	buffer := packet.Encode()
	err := c.memberlist.SendReliable(n.node, buffer)

	if err != nil {
		sentRequestFailed.Inc()
	}

	requestsSecondsSend.Observe(time.Since(start).Seconds())
	bytesSent.Add(float64(len(buffer)))

	start = time.Now()

	if err == nil {
		deadline := time.NewTimer(20 * time.Second)
		select {
		case reply = <-replyChan:
			if !deadline.Stop() {
				<-deadline.C
			}

			sentRequest.Inc()
		case err = <-replyError:
			if !deadline.Stop() {
				<-deadline.C
			}

			sentRequest.Inc()
		case <-deadline.C:
			err = errors.New("timed out")

			sentTimeout.Inc()
		}
	}

	c.mutex.Lock()
	delete(c.sentRPC, id)
	c.mutex.Unlock()

	requestsSecondsWaitReplay.Observe(time.Since(start).Seconds())

	return reply, err
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

func (c *Cluster) init() (err error) {
	c.notifyChan = make(chan []byte, 1000)
	c.requestToken = make(chan interface{}, concurrentRequest)
	c.sentRPC = make(map[uint32]rpcRequest)

	for i := 0; i < concurrentRequest; i++ {
		c.requestToken <- nil
	}

	c.wg.Add(1)

	go c.processNotify()

	cfg := memberlist.DefaultLANConfig()

	cfg.Delegate = delegate{
		nodeMeta: []byte(c.APIListenAddress),
		c:        c,
	}

	if c.ClusterPort != 0 {
		cfg.AdvertisePort = c.ClusterPort
		cfg.BindPort = c.ClusterPort
	}

	if c.ClusterAddress != "" {
		cfg.BindAddr = c.ClusterAddress
		cfg.AdvertiseAddr = c.ClusterAddress
	}

	cfg.Name = fmt.Sprintf("%s:%d", cfg.Name, cfg.BindPort)
	cfg.Logger = log.New(filteringLogger{}, "", 0)

	c.memberlist, err = memberlist.Create(cfg)
	if err != nil {
		return err
	}

	err = c.SeedProvider.SetAddress(c.memberlist.LocalNode().FullAddress().Addr, c.APIListenAddress)
	if err != nil {
		if err2 := c.memberlist.Shutdown(); err2 != nil {
			logger.Printf("Error during memberlist shutdown: %v", err)
		}

		return err
	}

	err = c.join()
	if err != nil {
		if err2 := c.memberlist.Shutdown(); err2 != nil {
			logger.Printf("Error during memberlist shutdown: %v", err)
		}

		return err
	}

	return nil
}

func (c *Cluster) join() error {
	inCluster := c.memberlist.NumMembers() > 1

	seeds, err := c.SeedProvider.Seeds()
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

		_, err := c.memberlist.Join(seeds[start:end])
		if err == nil {
			break
		}

		start = end
	}

	num := c.memberlist.NumMembers()
	numberNodes.Set(float64(num))

	if num > 1 && !inCluster {
		members := c.memberlist.Members()
		memberStrings := make([]string, len(members))

		for i, n := range members {
			memberStrings[i] = n.FullAddress().Addr
		}

		logger.Printf("Joinned a SquirrelDB cluster. Current nodes are %v", memberStrings)
	}

	return nil
}

func (c *Cluster) processNotify() {
	defer c.wg.Done()

	for msg := range c.notifyChan {
		c.processPacket(msg)
	}
}

func (c *Cluster) processPacket(msg []byte) {
	start := time.Now()

	defer func() {
		requestsSecondsRecv.Observe(time.Since(start).Seconds())
	}()

	packet, err := decode(msg)
	if err != nil {
		logger.Printf("Invalid RPC message: %v", err)
		messageBadFormat.Inc()

		return
	}

	if packet.PacketType == msgReply || packet.PacketType == msgError {
		c.mutex.Lock()
		rpc := c.sentRPC[packet.RequestID]
		c.mutex.Unlock()

		if rpc.replyChan == nil || rpc.replyError == nil {
			logger.Printf("Unknown RPC reply to id %d. Did I just restarted or the remote node is lagging ?", packet.RequestID)
			messageNoRequest.Inc()

			return
		}

		if packet.PacketType == msgReply {
			select {
			case rpc.replyChan <- packet.Payload:
				messageReply.Inc()
			default:
				logger.Printf("RPC reply arrived too late, dropping response for RPC id %d", packet.RequestID)
				messageNoRequest.Inc()
			}
		} else {
			select {
			case rpc.replyError <- errors.New(string(packet.Payload)):
				messageReplyErr.Inc()
			default:
				logger.Printf("RPC reply arrived too late, dropping response for RPC id %d", packet.RequestID)
				messageNoRequest.Inc()
			}
		}
	} else {
		c.mutex.Lock()
		closing := c.closing
		c.mutex.Unlock()

		if closing {
			messageAfterShutdown.Inc()
			return
		}

		<-c.requestToken
		go c.processRequest(packet)
	}
}

func (c *Cluster) processRequest(packet rpcPacket) {
	start := time.Now()

	defer func() {
		c.requestToken <- nil

		requestsSecondsProcessReq.Observe(time.Since(start).Seconds())
	}()

	c.mutex.Lock()
	callback := c.requestHandler
	c.mutex.Unlock()

	var (
		askingNode  *memberlist.Node
		replyPacket rpcPacket
	)

	nodes := c.memberlist.Members()
	for _, n := range nodes {
		if n.FullAddress().Addr == packet.ReplyToAddress {
			askingNode = n
		}
	}

	if askingNode == nil {
		logger.Printf("Received a request for unknown node %s", packet.ReplyToAddress)
		messageUnknownNode.Inc()

		return
	}

	if callback == nil {
		messageBeforeReady.Inc()

		replyPacket = rpcPacket{
			PacketType: msgError,
			Payload:    []byte("node is not ready"),
			RequestID:  packet.RequestID,
		}
	} else {
		messageRequest.Inc()
		payload, err := callback(packet.RequestType, packet.Payload)
		if err != nil {
			replyPacket = rpcPacket{
				PacketType: msgError,
				Payload:    []byte(err.Error()),
				RequestID:  packet.RequestID,
			}
		} else {
			replyPacket = rpcPacket{
				PacketType: msgReply,
				Payload:    payload,
				RequestID:  packet.RequestID,
			}
		}
	}

	err := c.memberlist.SendReliable(askingNode, replyPacket.Encode())
	if err != nil {
		logger.Printf("Failed to send reply: %v", err)
		sentReplyFailed.Inc()
	} else {
		sentReply.Inc()
	}
}
