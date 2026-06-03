package main

import (
	"encoding/json"
	"errors"
	"log"
	"maps"
	"math"
	"slices"
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
	ID       string
	Outbound []int
	Pending  []OutboundBroadcast
}

func (ob OutboundBroadcast) ShouldTry() bool {
	if ob.Msg.Message == nil && (ob.Msg.Messages == nil || len(*ob.Msg.Messages) == 0) {
		log.Println("found empty message")
		return false
	}
	if ob.LastAttempted == nil {
		return true
	}
	// exponentially backoff on retries but start with a high initial wait period to account for injected latency
	ms := int(math.Exp2(float64(ob.Attempts + 9)))
	nextTry := (*ob.LastAttempted).Add(time.Duration(ms) * time.Millisecond)
	return time.Now().After(nextTry)
}

func main() {
	receivedMsgs := map[int]bool{}
	rxMtx := sync.Mutex{}

	var nm NeighborMap

	n := maelstrom.NewNode()

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var rx BroadcastMsg
		if err := json.Unmarshal(msg.Body, &rx); err != nil {
			return err
		}

		if rx.Message == nil && rx.Messages == nil {
			return errors.New("no message found in broadcast")
		}
		rxMtx.Lock()
		defer rxMtx.Unlock()
		newMsgs := []int{}
		if rx.Messages != nil {
			for _, m := range *rx.Messages {
				if _, ok := receivedMsgs[m]; ok {
					continue
				}
				newMsgs = append(newMsgs, m)
			}
		}

		if rx.Message != nil {
			if _, ok := receivedMsgs[*rx.Message]; ok {
				rx.Type = "broadcast_ok"
				rx.Message = nil
				rx.Messages = nil
				return n.Reply(msg, rx)
			}
			newMsgs = append(newMsgs, *rx.Message)
		}

		for _, nm := range newMsgs {
			receivedMsgs[nm] = true
		}

		nm.Lock.Lock()
		for _, neighbor := range nm.Neighbors {
			if neighbor.ID == msg.Src {
				continue
			}
			nm.Neighbors[neighbor.ID] = Neighbor{
				ID:       neighbor.ID,
				Outbound: append(neighbor.Outbound, newMsgs...),
				Pending:  neighbor.Pending,
			}
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
		body["messages"] = slices.Collect(maps.Keys(receivedMsgs))
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
				ID:       neighbor,
				Outbound: []int{},
				Pending:  []OutboundBroadcast{},
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
				if len(neighbor.Outbound) > 0 {
					ob := OutboundBroadcast{
						LastAttempted: nil,
						Attempts:      0,
						Msg: BroadcastMsg{
							Type:     "broadcast",
							Messages: &neighbor.Outbound,
						},
					}
					pending := neighbor.Pending
					nm.Neighbors[neighbor.ID] = Neighbor{
						ID:       neighbor.ID,
						Outbound: []int{},
						Pending:  append(pending, ob),
					}
				}
				pending := neighbor.Pending
				for i, p := range pending {
					if p.ShouldTry() {
						if p.Attempts >= 1 {
							log.Printf("trying to broadcast %v for the %d time\n", *p.Msg.Messages, p.Attempts)
						}
						t := time.Now()
						p.LastAttempted = &t
						p.Attempts++
						err := n.RPC(neighbor.ID, p.Msg, func(mmsg maelstrom.Message) error {
							if mmsg.RPCError() == nil {
								nm.Lock.Lock()
								defer nm.Lock.Unlock()
								for j, sent := range nm.Neighbors[mmsg.Src].Pending {
									if sent.Msg.Messages == nil || p.Msg.Messages == nil {
										log.Println("got a bad message")
										continue
									}
									if slices.Equal(*sent.Msg.Messages, *p.Msg.Messages) {
										old := nm.Neighbors[mmsg.Src].Pending
										nm.Neighbors[mmsg.Src] = Neighbor{
											ID:       mmsg.Src,
											Outbound: nm.Neighbors[mmsg.Src].Outbound,
											Pending:  slices.Delete(old, j, j+1),
										}
									}
								}
							}
							return nil
						})
						if err != nil {
							log.Printf("got error sending message: %s\n", err)
							continue
						}
						pending[i] = p
					}
				}
			}
			nm.Lock.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
