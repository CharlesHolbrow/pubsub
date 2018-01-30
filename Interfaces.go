package pubsub

// Agent represents an object that can Subscribe to PubSub channels.
// Agents must have a random ID that can be retrieved with ID(). If a client
// connects, disconnects and then re-connects, it should have a different ID.
//
// It is your responsibility to ensure that no two Agents have the same ID.
//
// Note that the Receive method blocks the send loop, so agent Receive methods
// should be written to return quickly.
type Agent interface {
	Receive(channel string, data []byte) error
	ID() string
}

// A Receiver is any function that handles incoming messages from redis
type Receiver func(string, []byte)
