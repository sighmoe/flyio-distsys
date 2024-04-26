package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()

	n.Handle("send", sendHandler)
	n.Handle("poll", pollHandler)
	n.Handle("commit_offsets", commitOffsetsHandler)
	n.Handle("list_committed_offsets", listCommittedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return n.Reply(msg, body)
}

func pollHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return n.Reply(msg, body)
}

func commitOffsetsHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return nil
}

func listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return nil
}
