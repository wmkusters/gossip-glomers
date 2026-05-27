package main

import (
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMsg struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
	Message   *int   `json:"message,omitempty"`
}

func main() {
	msgs := []int{}
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var m BroadcastMsg
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		if m.Message == nil {
			return errors.New("nil message value in broadcast")
		}
		msgs = append(msgs, *m.Message)

		// Update the message type to return back.
		m.Type = "broadcast_ok"
		m.Message = nil
		return n.Reply(msg, m)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"
		body["messages"] = msgs
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delete(body, "topology")
		// Update the message type to return back.
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
