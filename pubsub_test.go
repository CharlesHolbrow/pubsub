package pubsub

import (
	"fmt"
	"testing"
)

type client struct {
	id string
}

func (c *client) Send(message string) error {
	fmt.Printf("client %s sending: %s\n", c.id, message)
	return nil
}
func (c *client) ID() string {
	return c.id
}

func TestPubSub_Subscribe(t *testing.T) {
	ps := NewPubSub()
	c1 := &client{"c1"}
	c2 := &client{"c2"}

	ps.Subscribe(c1, "0|0")
	channel, ok := ps.channels["0|0"]

	if !ok {
		t.Error("expected Subscribe() to create channel")
	}

	if channel["c1"] != c1 {
		t.Errorf("expected channel %s to be inserted, but found %s", *c1, channel["c1"])
	}

	var err error
	err = ps.Subscribe(c2, "0|0")

	if err != nil {
		t.Error("Got an error when we tried to subscribe a second agent", err)
	}

	// The channel should already exist
	if len(ps.channels) != 1 {
		t.Errorf("expected there to be a single channel")
	}
	if len(ps.channels["0|0"]) != 2 {
		t.Errorf("expected there to be two agents in the channel")
	}
	if len(ps.lists) != 2 {
		t.Errorf("Got channels %d subscribers, expected %d", len(ps.lists), 2)
	}

	// Subscribe to a second channel
	ps.Subscribe(c1, "1|1")
	if len(ps.lists["c1"]) != 2 {
		t.Errorf("Tried to subscribe to a second channel, but c1's subscription list has length=%d, (expected 2)", len(ps.lists["c1"]))
	}
	if len(ps.channels) != 2 {
		t.Errorf("Tried to subscribe to a seond channel, Expected there to be two channels, for %d", len(ps.channels))
	}

	// check state before unsubscribe
	if len(ps.channels["1|1"]) != 1 {
		t.Errorf("Expected 1|1 channel to have one agent, found %d", len(ps.channels["1|1"]))
	}
	if _, found := ps.lists["c1"]["1|1"]; !found {
		t.Errorf("Expected c1 list to have 1|1 subscriptions")
	}
	ps.Unsubscribe(c1, "1|1")
	// the channel should still exist, but it should not have agent c1 in it
	if len(ps.channels["1|1"]) != 0 {
		t.Errorf("Tried to unsubscribe, but 1|1 channel still has %d", len(ps.channels["1|1"]))
	}
	if len(ps.lists["c1"]) != 1 {
		t.Errorf("Tried to unsibscribe, but c1 list still has %d subscriptions", len(ps.lists["c1"]))
	}
	if _, found := ps.lists["c1"]["1|1"]; found {
		t.Errorf("Tried to unsubscribe, but still found 1|1 in c1 list")
	}
}
