package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n *maelstrom.Node
var kv *maelstrom.KV

func main() {
	n = maelstrom.NewNode()
	kv = maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		err := WriteDelta(n.ID(), delta)
		if err != nil {
			return err
		}

		go func() {
			n.Reply(msg, map[string]any{
				"type": "add_ok",
			})
		}()

		return nil
	})

	n.Handle("fanout", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counters := body["counters"].([]any)
		UpdateCounters(counters)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counters, err := getGlobalCounter()
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": counters,
		})
	})

	go fanout()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func WriteDelta(key string, delta int) error {
	read, err := kv.ReadInt(context.TODO(), key)
	if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
		return err
	}

	err = kv.Write(context.TODO(), key, read+delta)
	return err
}

func UpdateCounters(counters []any) error {
	for _, anyCounter := range counters {
		counter := anyCounter.(CounterState)
		read, err := kv.ReadInt(context.TODO(), counter.node)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}
		write := math.Max(float64(read), float64(counter.val))
		err = kv.Write(context.TODO(), counter.node, int(write))
		if err != nil {
			return err
		}
	}
	return nil
}

func fanout() {
	for _, dst := range n.NodeIDs() {
		if dst == n.ID() {
			continue
		}

		counters := getCounters()
		body := map[string]any{
			"type":     "fanout",
			"counters": counters,
		}

		n.Send(dst, body)
		time.Sleep(5)
	}
}

type CounterState struct {
	val  int
	node string
}

func getCounters() []CounterState {
	var counters []CounterState
	for _, node := range n.NodeIDs() {
		curr, err := kv.ReadInt(context.TODO(), node)
		if err != nil {
			curr = 0
		}
		counters = append(counters, CounterState{val: curr, node: node})
	}
	return counters
}

func getGlobalCounter() (int, error) {
	// writing a new value ensures seq-kv has seen all previous writes
	testKey := fmt.Sprintf("test-%d", time.Now().UnixNano())

	err := kv.Write(context.TODO(), testKey, time.Now().UnixNano())
	if err != nil {
		return 0, err
	}

	counter := 0
	for _, node := range n.NodeIDs() {
		val, err := kv.ReadInt(context.TODO(), node)
		if err != nil && maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			val = 0
		} else if err != nil {
			return 0, err
		}

		counter = counter + val
	}
	return counter, nil
}
