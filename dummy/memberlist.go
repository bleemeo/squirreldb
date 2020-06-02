package dummy

import "squirreldb/types"

type Memberlist struct {
	APIAddress string
}

type node struct {
	m *Memberlist
}

func (m Memberlist) Nodes() []types.Node {
	return []types.Node{node{&m}}
}

func (n node) APIAddress() string {
	return n.m.APIAddress
}

func (n node) ClusterAddress() string {
	return ""
}

func (n node) IsSelf() bool {
	return true
}
