package pubsub

import (
	"fmt"
	"sync"
)

// Agent represents an object that can Subscribe to PubSub channels.
// Agents must have a random ID that can be retrieved with ID(). If a client
// connects, disconnects and then re-connects, it should have a different ID.
//
// It is your responsibility to ensure that no two Agents have the same ID.
//
// Note that the Receive method blocks the send loop, so agent Receive methods
// should be written to return quickly.
type Agent interface {
	Receive([]byte) error
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

	// when we publish put all agents that returned errors here
	badAgents map[string]Agent

	lock sync.RWMutex
}

// NewPubSub creates a PubSub message broker
func NewPubSub() *PubSub {
	return &PubSub{
		channels:  make(map[string]pubSubChannel),
		lists:     make(map[string]pubSubList),
		badAgents: make(map[string]Agent),
	}
}

// pubSubChannel is a collection of subscribers organized by subscription key
type pubSubChannel map[string]Agent

// pubSubList is a collection of channels that an agent is subscribed to
type pubSubList map[string]pubSubChannel

// Publish calls subscriber.Receive(message) on each subscriber in the
// subscription channel identified by sKey. Safe for concurrent calls.
//
// Returns the number of errors encountered trying to publish.
//
// Agents that return a non-nil error on agent.Send() Will be added to an
// internal 'badAgents' collection.
func (ps *PubSub) Publish(sKey string, message []byte) int {
	ps.lock.RLock()
	badAgentCount := 0
	if psChannel, ok := ps.channels[sKey]; ok {
		for id, subscriber := range psChannel {
			err := subscriber.Receive(message)
			if err != nil {
				ps.badAgents[id] = subscriber
				badAgentCount++
			}
		}
	}
	ps.lock.RUnlock()
	return badAgentCount
}

// Add a client to a subscription key. Assumes you have a write Lock
func (ps *PubSub) subscribe(subscriber Agent, key string) (changed bool) {
	subscriberID := subscriber.ID()
	channel, ok := ps.channels[key]

	if !ok {
		channel = make(pubSubChannel)
		ps.channels[key] = channel
		changed = true
	}

	// check if we are already subscribed
	if _, ok := channel[subscriberID]; ok {
		return
	}

	channel[subscriberID] = subscriber
	// check if we already have a list of the
	subscriberSubscriptions, ok := ps.lists[subscriberID]

	if !ok {
		subscriberSubscriptions = make(pubSubList)
		ps.lists[subscriberID] = subscriberSubscriptions
	}

	subscriberSubscriptions[key] = channel

	return
}

// Subscribe the supplied Agent to channel identified by key. Returns true if
// a previously unsubscribed channel was created. Else return false.
func (ps *PubSub) Subscribe(agent Agent, key string) (changed bool) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.subscribe(agent, key)
}

// assumes you have a write lock
func (ps *PubSub) unsubscribe(agent Agent, key string) (changed bool) {
	agentID := agent.ID()

	// channel is a map of all agents subscribed to this key
	if channel, ok := ps.channels[key]; ok {
		delete(channel, agent.ID())
		if len(channel) == 0 {
			delete(ps.channels, key) // Channel has no subscribed agents
			changed = true
		}
	}

	// list is a collection of all the channels this agent is subscribed to
	if list, ok := ps.lists[agentID]; ok {
		delete(list, key)
		if len(list) == 0 {
			delete(ps.lists, agentID) // Agent is subscribed to 0 channels
		}
	}

	return
}

// Unsubscribe the supplied agent from the given channel.
func (ps *PubSub) Unsubscribe(agent Agent, key string) (changed bool) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.unsubscribe(agent, key)
}

// RemoveAgent removes an agent from ps and ubsubscribes it from all channels.
//
// Returns an error if the agent was not found in ps.lists
//
// It should be called every time an agent disconnects to ensure all traces of
// the agent are removed from ps.
func (ps *PubSub) RemoveAgent(agent Agent) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.removeAgent(agent)
}

func (ps *PubSub) removeAgent(agent Agent) error {
	agentID := agent.ID()

	list, found := ps.lists[agentID]

	if !found {
		return fmt.Errorf("Tried to remove agent (ID: %s) from all subscriptions, but subscription list not found", agentID)
	}

	// list is a collection of all the channels this agent is subscribed to
	for channelName, psChan := range list {
		// psChan is a map[idString]Agent
		delete(psChan, agentID)
		if len(psChan) == 0 {
			delete(list, channelName)
			delete(ps.channels, channelName)
		}

	}

	delete(ps.lists, agentID)
	return nil
}

// RemoveAllBadAgents finds all the agents labeled as "bad"
//
// Bad agents are returned in a collection, and a new empty badAgent collection
// is allocated.
//
// Returns nil if there are no Bad Agents
func (ps *PubSub) RemoveAllBadAgents() map[string]Agent {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	badAgents := ps.badAgents
	if len(badAgents) == 0 {
		return nil
	}
	ps.badAgents = make(map[string]Agent)
	for _, agent := range badAgents {
		// Note that we ignore errors returned by removeAgent. This should be
		// safe, because the error means that the agent was already removed.
		ps.removeAgent(agent)
	}

	return badAgents
}
