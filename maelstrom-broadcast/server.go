package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Service struct {
	n *maelstrom.Node

	data map[int]struct{}
	dMu  sync.RWMutex

	nodeId string
	id     int

	topology map[string][]string
	tMu      sync.RWMutex

	buf           []int
	lastBatchTime int64
}

func NewService(n *maelstrom.Node) *Service {
	service := &Service{n: n, data: make(map[int]struct{}), topology: make(map[string][]string), lastBatchTime: time.Now().UnixMilli()}
	return service
}

func (s *Service) broadcast(src string, body map[string]any) error {
	var neighbors []string
	s.tMu.RLock()
	neighbors = s.topology[s.nodeId]
	s.tMu.RUnlock()

	for _, dst := range neighbors {
		if dst == src || dst == s.nodeId {
			continue
		}

		go func() {
			retryCount := 1
			for {
				if err := s.rpc(dst, body); err != nil {
					time.Sleep(time.Duration(retryCount) * time.Second)
					retryCount = retryCount + 1
				} else {
					return
				}
			}
		}()
	}

	return nil
}

func (s *Service) batch(buf []int) error {
	var neighbors []string
	s.tMu.RLock()
	neighbors = s.topology[s.nodeId]
	s.tMu.RUnlock()

	for _, dst := range neighbors {
		if dst == s.nodeId {
			continue
		}

		go func() {
			body := map[string]any{
				"type":     "batch",
				"messages": buf,
			}
			retryCount := 1
			for {
				if err := s.rpc(dst, body); err != nil {
					time.Sleep(time.Duration(retryCount) * time.Second)
					retryCount = retryCount + 1
				} else {
					return
				}
			}
		}()
	}

	return nil
}

func (s *Service) rpc(dst string, body map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, dst, body)
	return err
}

func (s *Service) read() []int {
	s.dMu.RLock()
	var msgs []int
	for msg := range s.data {
		msgs = append(msgs, msg)
	}
	s.dMu.RUnlock()

	//sort.Ints(msgs)
	return msgs
}

// func (s *Service) updateTopology(topology map[string]any) error {
func (s *Service) updateTopology(overrideDefault bool, def map[string]any) error {
	s.tMu.Lock()
	defer s.tMu.Unlock()

	if !overrideDefault {
		s.topology = parseTopology(def)
	} else {
		// s.topology = rootTopology(len(s.n.NodeIDs()))
		s.topology = balancedTopology(len(s.n.NodeIDs()), 4)
	}
	return nil
}

// Some precomputed topology shapes based on network size
func rootTopology(numNodes int) map[string][]string {
	topology := make(map[string][]string)

	var children []string

	for i := 1; i < numNodes; i++ {
		child := fmt.Sprintf("n%d", i)
		topology[child] = []string{"n0"}
		children = append(children, child)
	}

	topology["n0"] = children
	return topology
}

func balancedTopology(numNodes int, branchFactor int) map[string][]string {
	curr := 0
	tree := make(map[string][]string)
	for i := 0; i < numNodes; i++ {
		if i%branchFactor == 0 {
			curr = curr + 1
		}
		node := fmt.Sprintf("n%d", curr)
		neighbor := fmt.Sprintf("n%d", i)
		tree[node] = append(tree[node], neighbor)
		tree[neighbor] = append(tree[neighbor], node)

		// connect nodes further down the branch back to the root for msg efficiency
		if i > branchFactor {
			tree[neighbor] = append(tree[neighbor], "n0")
		}
	}
	return tree
}

func parseTopology(topology map[string]any) map[string][]string {
	out := make(map[string][]string)

	for node, neighbors := range topology {
		for _, neighbor := range neighbors.([]any) {
			out[node] = append(out[node], neighbor.(string))
		}
	}

	return out
}
