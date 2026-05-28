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
	receivedMsgs := map[int]bool{}
	dedupedMsgs := []int{}
	rxMtx := sync.Mutex{}

	// map[neighbor] -> messages that need confirmation
	outbound := map[string]map[int]bool{}
	outboundMtx := sync.Mutex{}
	// this node's neighbors, set by Topology
	neighbors := []string{}
	n := maelstrom.NewNode()

	addToOutbound := func(neighbor string, msg int) {
		outboundMtx.Lock()
		if _, ok := outbound[neighbor]; !ok {
			outbound[neighbor] = map[int]bool{}
		}
		outbound[neighbor][msg] = true
		outboundMtx.Unlock()
	}

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
		rxMtx.Lock()
		defer rxMtx.Unlock()
		if _, ok := receivedMsgs[*rx.Message]; ok {
			rx.Type = "broadcast_ok"
			rx.Message = nil
			return n.Reply(msg, rx)
		}

		receivedMsgs[*rx.Message] = true
		dedupedMsgs = append(dedupedMsgs, *rx.Message)
		// stick this in a goroutine so we don't block responding
		for _, neighbor := range neighbors {
			go addToOutbound(neighbor, *rx.Message)
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
		body["messages"] = dedupedMsgs
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

	// outbound Tx loop
	go func() {
		for {
			newOutbound := map[string]map[int]bool{}
			outboundMtx.Lock()
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
			outboundMtx.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
