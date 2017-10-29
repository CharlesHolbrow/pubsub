package pubsub

import (
	"errors"
	"fmt"
	"testing"
)

type client struct {
	id  string // return this string on ID()
	err error  // Return this error on Send()
}

func (c *client) Send(message []byte) error {
	fmt.Printf("client %s sending: %s\n", c.id, message)
	return c.err
}
func (c *client) ID() string {
	return c.id
}

func TestPubSub_Subscribe(t *testing.T) {
	ps := NewPubSub()
	c1 := &client{"c1", nil}
	c2 := &client{"c2", nil}

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

	// Add a bunch of subscriptions, then remove the agent
	ps.Subscribe(c1, "0|1")
	ps.Subscribe(c1, "0|2")
	ps.Subscribe(c1, "0|3")
	ps.Publish("0|0", []byte("This is a test message"))

	if len(ps.lists["c1"]) != 4 {
		t.Errorf("Tried to add subscrbiptions to c1. Expected 4, but got %d", len(ps.lists["c1"]))
	}
	if len(ps.lists) != 2 {
		t.Errorf("Expected there to be two agents total, but found %d", len(ps.lists))
	}
	if len(ps.channels["0|1"]) != 1 {
		t.Errorf("Expected the new 0|1 channel to have 1 agent, but found %d", len(ps.channels["0|1"]))
	}

	ps.RemoveAgent(c1)
	if len(ps.lists) != 1 {
		t.Errorf("Expected there to be only one agent after removing c1, but found %d", len(ps.lists))
	}
	if len(ps.channels["0|1"]) != 0 {
		t.Errorf("Expeted the 0|1 channel to have no agents after removing c1, but found %d", len(ps.channels["0|1"]))
	}
}

func TestPubSub_RemoveAllBadAgents(t *testing.T) {
	ps := NewPubSub()
	c1 := &client{"c1", errors.New("expected error")}
	ps.Subscribe(c1, "0|0")
	ps.Publish("0|0", []byte("This is a test message"))

	// When an agent returns an error, it should be added to the badAgents list,
	// but not removed from ps or or ps.lists
	if agent, okay := ps.badAgents[c1.ID()]; !okay || agent != c1 {
		t.Errorf("Agent reutned an error on publish, but was not found in badAgents list. Instead we got %v, %v", agent, okay)
	}
	if _, okay := ps.lists[c1.ID()]; !okay {
		t.Error("Agent reutned an error on publish, but was not found in lists list")
	}
	if agent, okay := ps.channels["0|0"][c1.ID()]; !okay || agent != c1 {
		t.Error("Expected to find c1 in channel prior to RemoveAllBadAgents")
	}
	//
	badAgents := ps.RemoveAllBadAgents()
	if len(badAgents) != 1 || len(ps.badAgents) != 0 || len(ps.lists) != 0 {
		t.Error("Expected bad agents to be removed from ps, and returned")
	}
	if len(ps.channels["0|0"]) != 0 {
		t.Errorf("Expected channel to still exist, but agent to be removed after call to RemoveAllBadAgents. instead found %v", ps.channels["0|0"])
	}
}
