package pubsub

import (
	"fmt"
	"sync"
)

// Agent represents an object that can Subscribe to PubSub channels
type Agent interface {
	Send(string) error
	ID() string
}

// PubSub stores a collection of subscription keys, each containing a collection
// of subscribers. Methods should all be safe for concurrent calls.
type PubSub struct {
	// The subscription channels by subKey
	channels map[string]pubSubChannel

	// lists of subscriptions by their agent's IDs. Note that these are different
	// than the Subscriber interface. These are just a collection of
	// pubSubChannels
	lists map[string]pubSubList

	lock sync.RWMutex
}

// NewPubSub creates a PubSub message broker
func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]pubSubChannel),
		lists:    make(map[string]pubSubList),
	}
}

// pubSubChannel is a collection of subscribers organized by subscription key
type pubSubChannel map[string]Agent

// pubSubList is a collection of channels that an agent is subscribed to
type pubSubList map[string]pubSubChannel

// Publish calls subscriber.Send(message) on each subscriber in the subscription
// channel identified by sKey. Safe for concurrent calls.
func (ps *PubSub) Publish(sKey string, message string) {
	// Get the subscription channel.
	ps.lock.RLock()
	if psChannel, ok := ps.channels[sKey]; ok {
		for _, subscriber := range psChannel {
			subscriber.Send(message)
		}
	}
	ps.lock.RUnlock()
}

// Add a client to a subscription key. Assumes you have a write Lock
func (ps *PubSub) subscribe(subscriber Agent, key string) error {
	subscriberID := subscriber.ID()
	channel, ok := ps.channels[key]

	if !ok {
		channel = make(pubSubChannel)
		ps.channels[key] = channel
	}

	// check if we are already subscribed
	if _, ok := channel[subscriberID]; ok {
		return fmt.Errorf("Agent ID %s is already subscribed to %s", subscriberID, key)
	}

	channel[subscriberID] = subscriber
	// check if we already have a list of the
	subscriberSubscriptions, ok := ps.lists[subscriberID]

	if !ok {
		subscriberSubscriptions = make(pubSubList)
		ps.lists[subscriberID] = subscriberSubscriptions
	}

	subscriberSubscriptions[key] = channel

	return nil
}

// Subscribe the supplied Agent to channel identified by key
func (ps *PubSub) Subscribe(agent Agent, key string) error {
	ps.lock.Lock()
	changed := ps.subscribe(agent, key)
	ps.lock.Unlock()
	return changed
}

// assumes you have a write lock
func (ps *PubSub) unsubscribe(agent Agent, key string) {
	// list is a collection of all the channels this agent is subscribed to
	agentID := agent.ID()
	list := ps.lists[agentID]

	delete(list[key], agentID)
	delete(list, key)
}

// Unsubscribe the supplied agent from
func (ps *PubSub) Unsubscribe(agent Agent, key string) {
	ps.lock.Lock()
	ps.unsubscribe(agent, key)
	ps.lock.Unlock()
}

// RemoveAgent removes an agent from ps and ubsubscribes it from all channels.
//
// Returns an error if the agent was not found in ps.lists
//
// It should be called every time an agent disconnects to ensure all traces of
// the agent are removed from ps.
func (ps *PubSub) RemoveAgent(agent Agent) error {
	agentID := agent.ID()

	ps.lock.Lock()

	list, found := ps.lists[agentID]

	if !found {
		return fmt.Errorf("Tried to remove agent (ID: %s) from all subscriptions, but subscription list not found", agentID)
	}

	for _, psChan := range list {
		delete(psChan, agentID)
	}
	delete(ps.lists, agentID)

	ps.lock.Unlock()
	return nil
}
