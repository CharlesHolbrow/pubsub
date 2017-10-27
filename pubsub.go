package pubsub

import (
	"sync"
)

// Subscriber is any object that can we can send messages to
// Better way - just use RW Mutex and regular maps
type Subscriber interface {
	Send(string) error
	ID() string
}

// PubSub stores a collection of subscription keys, each containing a collection
// of subscribers. Methods should all be safe for concurrent calls.
type PubSub struct {
	// The subscription channels by subKey
	channels map[string]pubSubChannel

	// subscribers by their IDs. Note that these are different than the
	// Subscriber interface. These are just a collection of pubSubChannels
	subscribers map[string]pubSubSubscriber

	lock sync.RWMutex
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels:    make(map[string]pubSubChannel),
		subscribers: make(map[string]pubSubSubscriber),
	}
}

// pubSubChannel is a collection of subscribers organized by subscription key
type pubSubChannel map[string]Subscriber

// pubSubSubscriber is a collection of channels that a subscriber is subscribed to
type pubSubSubscriber map[string]pubSubChannel

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
func (ps *PubSub) subscribe(key string, subscriber Subscriber) (changed bool) {
	subscriberID := subscriber.ID()
	channel, ok := ps.channels[key]

	if !ok {
		channel = make(pubSubChannel)
		ps.channels[key] = channel
	}

	// check if we are already subscribed
	if _, ok := channel[subscriberID]; ok {
		return false
	}

	channel[subscriberID] = subscriber
	// check if we already have a list of the
	subscriberSubscriptions, ok := ps.subscribers[subscriberID]

	if !ok {
		subscriberSubscriptions = make(pubSubSubscriber)
		ps.subscribers[subscriberID] = subscriberSubscriptions
	}

	subscriberSubscriptions[key] = channel

	return true
}

func (ps *PubSub) Subscribe(key string, subscriber Subscriber) {
	ps.lock.Lock()
	ps.subscribe(key, subscriber)
	ps.lock.Unlock()
}

// func (ps *PubSub) AddSubscriber(sKey string, client Subscriber) {
// 	subscribersps.channels.LoadOrStore(sKey, sync.Map{}).(sync.Map)
// }

// type Client struct {
// 	subscriptions map[string]sync.Map
// }
