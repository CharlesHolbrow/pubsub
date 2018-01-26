package pubsub

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

// A Receiver is any function that handles incoming messages from redis
type Receiver func(string, []byte)

// RedisSubscription provides a nice way to subscribe to redis. An instance of
// this struct can subscribe and unsubscribe from many redis channels.
//
// New instances should be created with NewRedisSubscription(...)
//
// The subscribe and unsubscribe methods are synchronous.
type RedisSubscription struct {
	rps           *redis.PubSubConn
	rpsLocker     sync.Mutex
	pendignAdd    *keys
	pendingRem    *keys
	pendingLocker sync.RWMutex
	onReceive     Receiver
	flush         chan bool
	flushLocker   sync.Mutex
}

// NewRedisSubscription creates a RedisSubscription
// The calling code is responsible for closeing the redis connection
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

// Subscribe adds the supplied keys to our redis subscription. It does not
// return until the subscription is complete.
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
// causes all calls to .Subscribe to return once the subscription is complete.
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
	// because have yet to communicate with redis.

	if add != nil {
		rs.rps.Subscribe(add...)
	}
	if rem != nil {
		rs.rps.Unsubscribe(rem...)
	}

	// even if add and rem were nil, flush will allow goroutines waiting on
	// .Suspend() calls to return.
	flush <- true
}
