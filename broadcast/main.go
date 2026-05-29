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

type OutboundBroadcast struct {
	LastAttempted *time.Time
	Msg           BroadcastMsg
}

func main() {
	receivedMsgs := map[int]bool{}
	dedupedMsgs := []int{}
	rxMtx := sync.Mutex{}

	// map[neighbor] -> messages that need confirmation
	outbound := map[string]map[int]OutboundBroadcast{}
	outboundMtx := sync.Mutex{}
	// this node's neighbors, set by Topology
	neighbors := []string{}
	n := maelstrom.NewNode()

	addToOutbound := func(neighbor string, msg int) {
		outboundMtx.Lock()
		if _, ok := outbound[neighbor]; !ok {
			outbound[neighbor] = map[int]OutboundBroadcast{}
		}
		outbound[neighbor][msg] = OutboundBroadcast{
			LastAttempted: nil,
			Msg: BroadcastMsg{
				Type:    "broadcast",
				Message: &msg,
			},
		}
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
			outboundMtx.Lock()
			for neighbor, obs := range outbound {
				for msg, ob := range obs {
					if ob.LastAttempted != nil && ob.LastAttempted.Add(10*time.Millisecond).Before(time.Now()) {
						log.Printf("not retrying message with value %d yet from node %s\n", *ob.Msg.Message, n.ID())
						continue
					}
					t := time.Now()
					ob.LastAttempted = &t
					err := n.RPC(neighbor, ob.Msg, func(mmsg maelstrom.Message) error {
						log.Printf("callback handler called on node %s for msg: %d", n.ID(), msg)
						if mmsg.RPCError() == nil {
							outboundMtx.Lock()
							defer outboundMtx.Unlock()
							delete(outbound[neighbor], msg)
							return nil
						}
						return fmt.Errorf("rpc broadcast error on node %s for msg %d", n.ID(), msg)
					})
					if err != nil {
						log.Printf("got error sending message to neighbor: %s", err)
					}
					outbound[neighbor][msg] = ob
				}
			}
			outboundMtx.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
