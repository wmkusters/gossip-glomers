package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMsg struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
	Message   *int   `json:"message,omitempty"`
}

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology,omitempty"`
}

func main() {
	msgs := map[int]bool{}
	ml := sync.Mutex{}
	neighbors := []string{}
	outbound := map[string]map[int]bool{}
	omu := sync.Mutex{}
	n := maelstrom.NewNode()

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var rx BroadcastMsg
		if err := json.Unmarshal(msg.Body, &rx); err != nil {
			return err
		}

		if rx.Message == nil {
			return errors.New("nil message value in broadcast")
		}
		ml.Lock()
		defer ml.Unlock()
		if _, ok := msgs[*rx.Message]; ok {
			return nil
		}

		msgs[*rx.Message] = true
		omu.Lock()
		defer omu.Unlock()
		for _, neighbor := range neighbors {
			if _, ok := outbound[neighbor]; !ok {
				outbound[neighbor] = map[int]bool{}
			}
			outbound[neighbor][*rx.Message] = true
		}
		// Update the message type to return back.
		rx.Type = "broadcast_ok"
		rx.Message = nil
		return n.Reply(msg, rx)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"
		tx := []int{}
		ml.Lock()
		defer ml.Unlock()
		for m := range msgs {
			tx = append(tx, m)
		}
		body["messages"] = tx
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var m TopologyMsg
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		neighbors = m.Topology[n.ID()]
		reply := TopologyMsg{
			Type:     "topology_ok",
			Topology: nil,
		}
		return n.Reply(msg, reply)
	})

	go func() {
		for {
			newOutbound := map[string]map[int]bool{}
			omu.Lock()
			for neighbor, ob := range outbound {
				outbound[neighbor] = map[int]bool{}
				for msg := range ob {
					outbound[neighbor][msg] = true
				}
			}

			for neighbor, ob := range outbound {
				for msg := range ob {
					err := n.RPC(neighbor, BroadcastMsg{
						Type:    "broadcast",
						Message: &msg,
					}, func(mmsg maelstrom.Message) error {
						log.Printf("callback handler called on node %s for msg: %d", n.ID(), msg)
						if mmsg.RPCError() == nil {
							delete(newOutbound[neighbor], msg)
							return nil
						}
						return fmt.Errorf("rpc broadcast error on node %s for msg %d", n.ID(), msg)
					})
					if err != nil {
						log.Printf("got error sending message to neighbor: %s", err)
					}
				}
			}
			omu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
