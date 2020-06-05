package memberlist

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
			num := m.memberlist.NumMembers()
			numberNodes.Set(float64(num))

			if num == 1 {
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

	m.mutex.Lock()
	m.closing = true
	m.mutex.Unlock()

	// Give short delay or it's likely that memberlist complain about "use of closed network connection"
	time.Sleep(100 * time.Millisecond)

	err = m.memberlist.Shutdown()
	if err != nil {
		logger.Printf("Unable to shutdown memberlist: %v", err)
	}

	numberNodes.Set(0)

	// Give short delay because Shutdown return *before* memberlist really stopped :(
	time.Sleep(100 * time.Millisecond)

	// Wait for completion of every pending request.
	for n := 0; n < concurrentRequest; n++ {
		<-m.requestToken
	}
	close(m.notifyChan)
	m.wg.Wait()
}

type node struct {
	apiAddress     string
	clusterAddress string
	isSelf         bool
	node           *memberlist.Node
}

// Nodes return list of known nodes of the cluster, in a consistent order.
func (m *Memberlist) Nodes() []types.Node {
	start := time.Now()

	defer func() {
		requestsSecondsNode.Observe(time.Since(start).Seconds())
	}()

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

func (m *Memberlist) Send(n types.Node, requestType uint8, data []byte) ([]byte, error) {
	return m.sendUsingCluster(n, requestType, data)
}

func (m *Memberlist) SetRequestHandler(f func(requestType uint8, data []byte) ([]byte, error)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.requestHandler = f
}

// sendUsingCluster send message (and wait reply using cluster messaging).
func (m *Memberlist) sendUsingCluster(tmp types.Node, requestType uint8, data []byte) ([]byte, error) {
	start := time.Now()

	n, ok := tmp.(node)
	if !ok {
		return nil, errors.New("node is not a object from memberlist package. Can't use it")
	}

	replyChan := make(chan []byte)
	replyError := make(chan error)

	m.mutex.Lock()
	id := m.nextRequestID
	m.nextRequestID++
	m.sentRPC[id] = rpcRequest{
		sentTime:   time.Now(),
		replyChan:  replyChan,
		replyError: replyError,
	}
	m.mutex.Unlock()

	mySelf := m.memberlist.LocalNode()
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
	err := m.memberlist.SendReliable(n.node, buffer)

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

	m.mutex.Lock()
	delete(m.sentRPC, id)
	m.mutex.Unlock()

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

func (m *Memberlist) init() (err error) {
	m.notifyChan = make(chan []byte, 1000)
	m.requestToken = make(chan interface{}, concurrentRequest)
	m.sentRPC = make(map[uint32]rpcRequest)

	for i := 0; i < concurrentRequest; i++ {
		m.requestToken <- nil
	}

	m.wg.Add(1)

	go m.processNotify()

	cfg := memberlist.DefaultLANConfig()

	cfg.Delegate = delegate{
		nodeMeta: []byte(m.APIListenAddress),
		m:        m,
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

	num := m.memberlist.NumMembers()
	numberNodes.Set(float64(num))

	if num > 1 && !inCluster {
		members := m.memberlist.Members()
		memberStrings := make([]string, len(members))

		for i, n := range members {
			memberStrings[i] = n.FullAddress().Addr
		}

		logger.Printf("Joinned a SquirrelDB cluster. Current nodes are %v", memberStrings)
	}

	return nil
}

func (m *Memberlist) processNotify() {
	defer m.wg.Done()

	for msg := range m.notifyChan {
		m.processPacket(msg)
	}
}

func (m *Memberlist) processPacket(msg []byte) {
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
		m.mutex.Lock()
		rpc := m.sentRPC[packet.RequestID]
		m.mutex.Unlock()

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
		m.mutex.Lock()
		closing := m.closing
		m.mutex.Unlock()

		if closing {
			messageAfterShutdown.Inc()
			return
		}

		<-m.requestToken
		go m.processRequest(packet)
	}
}

func (m *Memberlist) processRequest(packet rpcPacket) {
	start := time.Now()

	defer func() {
		m.requestToken <- nil

		requestsSecondsProcessReq.Observe(time.Since(start).Seconds())
	}()

	m.mutex.Lock()
	callback := m.requestHandler
	m.mutex.Unlock()

	var (
		askingNode  *memberlist.Node
		replyPacket rpcPacket
	)

	nodes := m.memberlist.Members()
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

	err := m.memberlist.SendReliable(askingNode, replyPacket.Encode())
	if err != nil {
		logger.Printf("Failed to send reply: %v", err)
		sentReplyFailed.Inc()
	} else {
		sentReply.Inc()
	}
}
