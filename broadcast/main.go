package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const BundleSize = 10

type BroadcastMsg struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
	Message   *int   `json:"message,omitempty"`
	Messages  *[]int `json:"messages,omitempty"`
}

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology,omitempty"`
}

type OutboundBroadcast struct {
	LastAttempted *time.Time
	Attempts      int
	Msg           BroadcastMsg
}

type NeighborMap struct {
	Lock      sync.Mutex
	Neighbors map[string]Neighbor
}

type Neighbor struct {
	ID     string
	Needed []OutboundBroadcast
}

func (ob OutboundBroadcast) ShouldTry() bool {
	if ob.LastAttempted == nil {
		return true
	}
	// exponentially backoff on retries but start with a high initial wait period to account for injected latency
	ms := int(math.Exp2(float64(ob.Attempts + 7)))
	nextTry := (*ob.LastAttempted).Add(time.Duration(ms) * time.Millisecond)
	return time.Now().After(nextTry)
}

func main() {
	receivedMsgs := map[int]bool{}
	dedupedMsgs := []int{}
	rxMtx := sync.Mutex{}

	var nm NeighborMap

	n := maelstrom.NewNode()

	addToOutbound := func(nid string, msg int) {
		outbound := nm.Neighbors[nid].Needed
		ob := OutboundBroadcast{
			LastAttempted: nil,
			Attempts:      0,
			Msg: BroadcastMsg{
				Type:     "broadcast",
				Messages: &[]int{},
			},
		}
		if len(outbound) == 0 ||
			(outbound[len(outbound)-1].Msg.Messages != nil && len(*outbound[len(outbound)-1].Msg.Messages) < BundleSize) {
			outbound = append(outbound, ob)
		} else {
			ob = outbound[len(outbound)-1]
		}
		new := []int{}
		if ob.Msg.Messages != nil {
			new = *ob.Msg.Messages
		}
		new = append(new, msg)
		ob.Msg.Messages = &new
		outbound[len(outbound)-1] = ob
		nm.Neighbors[nid] = Neighbor{
			ID:     nid,
			Needed: outbound,
		}
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
		nm.Lock.Lock()
		for _, neighbor := range nm.Neighbors {
			if neighbor.ID == msg.Src {
				continue
			}
			addToOutbound(neighbor.ID, *rx.Message)
		}
		nm.Lock.Unlock()

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
		rxMtx.Lock()
		defer rxMtx.Unlock()
		body["messages"] = dedupedMsgs
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var m TopologyMsg
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		if _, ok := m.Topology[n.ID()]; !ok {
			panic("can't find self in topology")
		}
		nm = NeighborMap{
			Lock:      sync.Mutex{},
			Neighbors: map[string]Neighbor{},
		}
		for _, neighbor := range m.Topology[n.ID()] {
			nm.Neighbors[neighbor] = Neighbor{
				ID:     neighbor,
				Needed: []OutboundBroadcast{},
			}
		}
		reply := TopologyMsg{
			Type:     "topology_ok",
			Topology: nil,
		}
		return n.Reply(msg, reply)
	})

	type readBody struct {
		Messages []int `json:"messages,omitempty"`
	}

	// outbound Tx loop
	go func() {
		for {
			nm.Lock.Lock()
			for _, neighbor := range nm.Neighbors {
				for _, ob := range neighbor.Needed {
					log.Printf("considering sending message with id %d to neighbor %s\n", ob.Msg.MsgID, neighbor.ID)
					if !ob.ShouldTry() {
						log.Printf("not retrying message with id %d yet from node %s\n", ob.Msg.MsgID, n.ID())
						continue
					}
					t := time.Now()
					ob.LastAttempted = &t
					ob.Attempts++
					err := n.RPC(neighbor.ID, ob.Msg, func(mmsg maelstrom.Message) error {
						if mmsg.RPCError() == nil {
							nm.Lock.Lock()
							defer nm.Lock.Unlock()
							var j *int
							for idx, nmsg := range neighbor.Needed {
								var b maelstrom.MessageBody
								err := json.Unmarshal(mmsg.Body, &b)
								if err != nil {
									return err
								}
								if nmsg.Msg.MsgID == b.InReplyTo {
									j = &idx
								}
							}
							if j != nil {
								nm.Neighbors[neighbor.ID] = Neighbor{
									ID:     neighbor.ID,
									Needed: append(nm.Neighbors[neighbor.ID].Needed[:*j], nm.Neighbors[neighbor.ID].Needed[*j+1:]...),
								}
							}
							return nil
						}
						return fmt.Errorf("rpc broadcast error on node %s for msg %d", n.ID(), ob.Msg.MsgID)
					})
					if err != nil {
						log.Printf("got error sending message to neighbor: %s", err)
					}
				}
			}
			nm.Lock.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
