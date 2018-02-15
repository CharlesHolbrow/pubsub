package pubsub

import (
	"fmt"
	"sync"
)

// PubSub stores a collection of channels, each containing a collection
// of subscribers. Methods should all be safe for concurrent calls.
//
// PubSub keeps track of the channels that we subscribe and unsubscribe to.
// Notice how the  `Flush() (add, rem []string)` method can be used to get
// the difference between the current subscription, and the subscrition at
// the last call to `Flush()`.
type PubSub struct {
	// Lock for channels, emptyChannels, lists and badAgents, pendingAdd/Rem
	lock sync.RWMutex

	// The subscription channels by subKey
	channels map[string]pubSubChannel

	// lists of subscriptions by their agent's IDs. Note that these are different
	// than the Subscriber interface. These are just a collection of
	// pubSubChannels
	lists map[string]pubSubList

	// when we publish put all agents that returned errors here
	badAgents map[string]Agent

	// Diff since last resolve. True means Added. False means removed.
	pending map[string]bool
}

// NewPubSub creates a PubSub message broker
func NewPubSub() *PubSub {
	return &PubSub{
		channels:  make(map[string]pubSubChannel),
		lists:     make(map[string]pubSubList),
		badAgents: make(map[string]Agent),
		pending:   make(map[string]bool),
	}
}

// pubSubChannel is a collection of subscribers organized by agent id
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

// Add a client to a channel. Assumes you have a write Lock
func (ps *PubSub) subscribe(subscriber Agent, channelName string) {
	subscriberID := subscriber.ID()
	channel, ok := ps.channels[channelName]

	if !ok {
		// we need to create a new channel
		channel = make(pubSubChannel)
		ps.channels[channelName] = channel

		if pending, ok := ps.pending[channelName]; ok && !pending {
			// we added this channel since the last flush
			delete(ps.pending, channelName)
		} else if !ok {
			ps.pending[channelName] = true
		} else if ok && pending {
			panic("pubsub.PubSub.subscribe found an invalid state")
		}
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

	subscriberSubscriptions[channelName] = channel

	return
}

// Subscribe the supplied Agent to channel identified by channelName.
func (ps *PubSub) Subscribe(agent Agent, channelName string) {
	ps.lock.Lock()
	ps.subscribe(agent, channelName)
	ps.lock.Unlock()
}

// assumes you have a write lock
func (ps *PubSub) unsubscribe(agent Agent, channelName string) {
	agentID := agent.ID()

	// channel is a map of all agents subscribed to this channelName
	if channel, ok := ps.channels[channelName]; ok {
		delete(channel, agent.ID())

		// Is the channel empty?
		if len(channel) == 0 {
			// Channel has no subscribed agents. Delete it.
			delete(ps.channels, channelName)
			// Update the pending diff
			if pending, ok := ps.pending[channelName]; ok && pending {
				// The channel was added after the last flush
				delete(ps.pending, channelName)
			} else if !ok {
				// We need to send a 'remove' message for this channel.
				ps.pending[channelName] = false
			} else if ok && !pending {
				panic("pubsub.PubSub.unsubscribe with an invalid state.")
			}
		}
	}

	// list is a collection of all the channels this agent is subscribed to
	if list, ok := ps.lists[agentID]; ok {
		delete(list, channelName)
		if len(list) == 0 {
			delete(ps.lists, agentID) // Agent is subscribed to 0 channels
		}
	}

	return
}

// Unsubscribe the supplied agent from the given channel.
func (ps *PubSub) Unsubscribe(agent Agent, channelName string) {
	ps.lock.Lock()
	ps.unsubscribe(agent, channelName)
	ps.lock.Unlock()
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
		// For every channel that the agent is subscribed to...

		// Remove the channel from the agent's channel list. This might not
		// actually be needed, because we remove the entire agent list below.
		delete(list, channelName)

		// psChan is a map[idString]Agent
		delete(psChan, agentID) // for each channel, remove the agent

		// is the channel now empty?
		if len(psChan) == 0 {
			// Channel has 0 agents.
			delete(ps.channels, channelName)
			// Update the pending diff
			if pending, ok := ps.pending[channelName]; ok && pending {
				// The channel was added after the last flush
				delete(ps.pending, channelName)
			} else if !ok {
				// We need to send a 'remove' message for this channel.
				ps.pending[channelName] = false
			} else if ok && !pending {
				panic("pubsub.PubSub.unsubscribe with an invalid state.")
			}
		}
	}

	delete(ps.lists, agentID)
	return nil
}

// RemoveAllBadAgents finds and removes all the agents labeled as "bad".
//
// Bad agents are returned in a collection, and a new empty badAgent collection
// is allocated. Nil if there are no Bad Agents.
func (ps *PubSub) RemoveAllBadAgents() map[string]Agent {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.removeAllBadAgents()
}

// assumes you have a write lock.
func (ps *PubSub) removeAllBadAgents() map[string]Agent {
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

// Flush returns a diff since the last call to flush, of all the channels that
// have been added or removed. The result should be accurate even if sent subscribe
// or unsubscribe request redundantly.
//
// If there are no changes, add and rem will both be nil.
func (ps *PubSub) Flush() (add, rem []string) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if len(ps.pending) == 0 {
		return
	}
	add = make([]string, 0, len(ps.pending))
	rem = make([]string, 0, len(ps.pending))

	for pending, tf := range ps.pending {
		if tf {
			add = append(add, pending)
		} else {
			rem = append(rem, pending)
		}
	}
	ps.pending = make(map[string]bool)
	return
}
