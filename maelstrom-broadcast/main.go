package main

import (
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := NewService(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		id, err := strconv.Atoi(n.ID()[1:])
		if err != nil {
			return err
		}
		s.id = id
		s.nodeId = n.ID()
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// optimization attempt: check if we have this msg already and short circuit
		val := int(body["message"].(float64))
		s.dMu.Lock()
		if _, ok := s.data[val]; ok {
			s.dMu.Unlock()
			return nil
		}
		s.data[val] = struct{}{}
		s.dMu.Unlock()

		// reply async that we have persisted value and start broadcast
		go func() {
			n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}()

		return s.broadcast(msg.Src, body)
	})

	n.Handle("batch", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		vals := []int{}
		msgs := body["messages"].([]any)
		for s := range msgs {
			vals = append(vals, s)
		}
		s.dMu.Lock()
		for val := range vals {
			s.data[val] = struct{}{}
		}
		s.dMu.Unlock()
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := s.read()
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": msgs,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		err := s.updateTopology(true, body["topology"].(map[string]any))
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
