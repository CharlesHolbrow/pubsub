package pubsub

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

// A Receiver is any function that handles incoming messages from redis
type Receiver func(string, []byte)

// RedisSubscription provides a nice way to subscribe to redis. This allows us
// to use a single redis connection to subscribe and unsubscribe to many
// different redis channels.
// - Subscribe method is synchronous (it blocks until then next call to Flush)
// - Unsubscribe method ques channels to be unsubscribed, but does not block
// - Flush method resolves all pending subscribes and unsubscribes
// - All exported methods are safe for concurrent calls.
//
// Panics if there is an error with redis connection.
//
// New instances should be created with NewRedisSubscription(...)
type RedisSubscription struct {
	rps           *redis.PubSubConn
	rpsLocker     sync.Mutex
	pendignAdd    *keys
	pendingRem    *keys
	pendingLocker sync.Mutex
	onReceive     Receiver
	flush         chan bool
	flushLocker   sync.Mutex
}

// NewRedisSubscription creates and initializes a RedisSubscription
// Panic on error receiveing from redis (and close conn)
func NewRedisSubscription(conn redis.Conn, onReceive Receiver) *RedisSubscription {
	sub := &RedisSubscription{
		rps:        &redis.PubSubConn{Conn: conn},
		pendignAdd: newKeys(),
		pendingRem: newKeys(),
		onReceive:  onReceive,
		flush:      make(chan bool),
	}

	// pipe all the receive calls to the onReceive method
	go func() {
		defer sub.rps.Close()
		for {
			switch v := sub.rps.Receive().(type) {
			case redis.Message:
				sub.onReceive(v.Channel, v.Data)
			case error:
				panic("Error encountered in RedisSubscription: " + v.Error())
			case redis.PMessage:
				// pattern message
			case redis.Subscription:
				// Redis is confirming our subscription v.Channel, v.Kind, v.Count
			}
		}
	}()

	return sub
}

// Subscribe adds the supplied keys to our redis subscription. Block until the
// next call to Flush()
func (rs *RedisSubscription) Subscribe(keys ...string) {
	rs.pendingLocker.Lock()

	rs.pendingRem.rem(keys...) // write lock
	rs.pendignAdd.add(keys...) // write lock

	flush := rs.flush
	rs.pendingLocker.Unlock()
	<-flush // wait for flush....
}

// Unsubscribe removes the supplied keys from our redis subscription. Unlike
// Subscribe, it returns immediately. The included keys will be removed at the
// next call to .Flush()
func (rs *RedisSubscription) Unsubscribe(keys ...string) {
	rs.pendingLocker.Lock()
	rs.pendignAdd.rem(keys...) // write lock
	rs.pendingRem.add(keys...) // write lock
	rs.pendingLocker.Unlock()
}

// Flush rationalizes all pending Subscribe and Unsubscribe requests. It also
// allows all calls to .Subscribe to return once the subscription is complete.
//
// It only connects to redis IF there are pending changes. (My goal is to make
// calls to Subscribe as light-weight as possible)
func (rs *RedisSubscription) Flush() {
	// I think it's worth locking redis pubsub for the entire duration of the
	// flush call. This ensures that Flush is safe for concurrent calls
	rs.rpsLocker.Lock()
	defer rs.rpsLocker.Unlock()

	// Lock this object while we mutate it
	rs.pendingLocker.Lock()
	add := rs.pendignAdd.clear()
	rem := rs.pendingRem.clear()
	flush := rs.flush
	rs.flush = make(chan bool)
	rs.pendingLocker.Unlock()
	// We are done mutating this struct. But we cannot unlock rpsLocker yet,
	// because we have not yet communicated with redis.

	// If both add and rem are empty, we do not need to talk to redis at all.
	// If we do need to talk to redis, set pending=true. NOTE: len(nil) == 0.
	var pending bool
	var err error

	if len(rem) > 0 {
		if err = rs.rps.Conn.Send("UNSUBSCRIBE", rem...); err != nil {
			panic("Error sending UNSUBSCRIBE message to redis:" + err.Error())
		}
		pending = true
	}
	if len(add) > 0 {
		if err = rs.rps.Conn.Send("SUBSCRIBE", add...); err != nil {
			panic("Error sending SUBSCRIBE message to redis: " + err.Error())
		}
		pending = true
	}
	if pending {
		// We need to update our subscription with redis.
		rs.rps.Conn.Flush()
		if _, err := rs.rps.Conn.Receive(); err != nil {
			panic("Error during redis subscription: " + err.Error())
		}
	}

	// even if add and rem were nil, flush will allow goroutines waiting on
	// .Suspend() calls to return.
	flush <- true
}
