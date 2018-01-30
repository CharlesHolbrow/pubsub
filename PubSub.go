package pubsub

import (
	"fmt"
	"sync"
)

// PubSub stores a collection of subscription keys, each containing a collection
// of subscribers. Methods should all be safe for concurrent calls.
type PubSub struct {
	// The subscription channels by subKey
	channels map[string]pubSubChannel

	// When we remove an agent, we want to keep track of which channels are now
	// empty. For each empty channel, we will add an entry to this collection.
	emptyChannels map[string]pubSubChannel

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
		channels:      make(map[string]pubSubChannel),
		emptyChannels: make(map[string]pubSubChannel),
		lists:         make(map[string]pubSubList),
		badAgents:     make(map[string]Agent),
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
			err := subscriber.Receive(sKey, message)
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
		// Do we already have this channel in the emptyChannels collection?
		if emptyChannel, ok := ps.channels[key]; ok {
			// The channel already exists, we just have to move it from the
			// emptyChannels collection back to the channels collection.
			channel = emptyChannel
			delete(ps.emptyChannels, key)
			// Do not set changed = true. We did not actually change anything.
		} else {
			// we need to create a new channel
			channel = make(pubSubChannel)
			changed = true
		}

		ps.channels[key] = channel
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

// For each channel that is emptied as a result of this operation, add that
// channel to ps.emptyChannels
func (ps *PubSub) removeAgent(agent Agent) error {
	agentID := agent.ID()

	list, found := ps.lists[agentID]

	if !found {
		return fmt.Errorf("Tried to remove agent (ID: %s) from all subscriptions, but subscription list not found", agentID)
	}

	// list is a collection of all the channels this agent is subscribed to
	for channelName, psChan := range list {
		// For every channel that the agent is subscribed to...

		// Remove the channel from the agent's channel list. This might not
		// actually be needed, because we remove the entire agent list below.
		delete(list, channelName)

		// psChan is a map[idString]Agent
		delete(psChan, agentID) // for each channel, remove the agent

		// is the channel now empty?
		if len(psChan) == 0 {
			// Channel has 0 agents.
			// Move channel to emptyChannels
			delete(ps.channels, channelName)
			ps.emptyChannels[channelName] = psChan
		}
	}

	delete(ps.lists, agentID)
	return nil
}

// RemoveAllBadAgents finds and removes all the agents labeled as "bad".
//
// Bad agents are returned in a collection, and a new empty badAgent collection
// is allocated. Nil if there are no Bad Agents.
//
// Removed Channels will be listed in the []string. Will be nil if no channels
// were removed.
func (ps *PubSub) RemoveAllBadAgents() (map[string]Agent, []string) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	badAgents := ps.badAgents
	if len(badAgents) == 0 {
		// Is it possible to have 0 bad agents, but to have one or more empty
		// channels? I'm thinking this is not possible, but it could use a bit
		// more thought. For now, if there are no bad agents, we simply return
		// nil for both the agent list and for the emptied channels.
		return nil, nil
	}
	ps.badAgents = make(map[string]Agent)
	for _, agent := range badAgents {
		// Note that we ignore errors returned by removeAgent. This should be
		// safe, because the error means that the agent was already removed.
		ps.removeAgent(agent)
	}

	// If we have any emptied channels, store their names in a string slice.
	var emptyChannels []string
	if len(ps.emptyChannels) > 0 {
		emptyChannels = make([]string, 0, len(ps.emptyChannels))
		for channelName := range ps.emptyChannels {
			emptyChannels = append(emptyChannels, channelName)
		}
		ps.emptyChannels = make(map[string]pubSubChannel)
	}

	return badAgents, emptyChannels
}
