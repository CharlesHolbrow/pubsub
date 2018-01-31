package pubsub

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

// RedisSubscription provides a nice way to subscribe to redis.
//
// Panics if there is an error with redis connection.
//
// New instances should be created with NewRedisSubscription(...)
type RedisSubscription struct {
	// redigo connections are not concurrency safe, so we lock access
	sync.Mutex
	rps *redis.PubSubConn

	// keep track of if the user requested we close the connection
	closeRequested *toggle
}

// NewRedisSubscription creates and initializes a RedisSubscription
//
// Panics if it receives an error from redis.
// Automatically closes conn when we receive an error, or call .Close()
func NewRedisSubscription(conn redis.Conn, onReceive Receiver) *RedisSubscription {
	rs := &RedisSubscription{
		rps:            &redis.PubSubConn{Conn: conn},
		closeRequested: &toggle{},
	}

	// Pipe all the receive calls to the onReceive method
	go func() {
		// From reading the source code, I believe the redigo conn.Close()
		// method IS safe for concurrent calls, but the redigo docs are
		// unclear. It is safe to call redigo.Conn.Close() more than once
		// (subsequent calls will return a non-nil error).
		defer rs.rps.Close()
		for {
			switch v := rs.rps.Receive().(type) {
			case redis.Message:
				onReceive(v.Channel, v.Data)
			case error:
				if rs.closeRequested.Get() {
					return
				}
				if rs.rps.Conn.Err() == nil {
					// This is a recoverable error. I considered calling
					// sub.rps.Unsubscribe() here, but we are in the receieving
					// goroutine, not the sending goroutine, so I think it's
					// better to just give up on the connection, because we are
					// not allowed to send anything from this goroutine. This
					// means we should never use Conn from a connection pool.
					panic("RedisSubscription: Error encountered: " + v.Error())
				} else {
					// unrecoverable error. for example, someone closed the connection
					panic("RedisSubscription: Connection error encountered: " + v.Error())
				}
			case redis.PMessage:
				// pattern message
			case redis.Subscription:
				// Redis is confirming our subscription v.Channel, v.Kind ("subscribe"), v.Count
			}
		}
	}()

	return rs
}

// Update the subscription by adding and removing channel Names
//
// First remove remNames from the subscription, then add addNames.
//
// It only connects to redis IF there are pending changes. (My goal is to make
// calls to Subscribe as light-weight as possible)
func (rs *RedisSubscription) Update(addNames, remNames []string) {
	// I think it's worth locking redis pubsub for the entire duration of the
	// flush call. This ensures that Flush is safe for concurrent calls
	rs.Lock()
	defer rs.Unlock()

	// If both add and rem are empty, we do not need to talk to redis at all.
	// If we do need to talk to redis, set pending=true. NOTE: len(nil) == 0.
	var pending bool
	var err error

	if len(remNames) > 0 {
		rem := make([]interface{}, len(remNames))
		for i, name := range remNames {
			rem[i] = name
		}
		if err = rs.rps.Conn.Send("UNSUBSCRIBE", rem...); err != nil {
			panic("Error sending UNSUBSCRIBE message to redis:" + err.Error())
		}
		pending = true
	}

	if len(addNames) > 0 {
		add := make([]interface{}, len(addNames))
		for i, name := range addNames {
			add[i] = name
		}
		if err = rs.rps.Conn.Send("SUBSCRIBE", add...); err != nil {
			panic("Error sending SUBSCRIBE message to redis: " + err.Error())
		}
		pending = true
	}

	if pending {
		// We need to update our subscription with redis.
		if err = rs.rps.Conn.Flush(); err != nil {
			panic("Error during flush to send redis subscription: " + err.Error())
		}
		// Note that we do not rs.rps.Conn.Receive() here,
		// because this is handled by the redis.PubSubConn

		// IMPORTANT NOTE ABOUT SCALING REDIS
		// I'm not currently waiting for confirmation that we are actually
		// subscribed. I think this is fine for when we are using a single
		// instance of redis. My current understanding is that because redis is
		// single threaded we can assume that once the call to Flush() returns
		// we will receive all future published messages on the specified
		// channels. If redis is running in cluster mode I would need to think
		// about this more. At the very least I would want to wait until we
		// received the subscription confirmation from redis before closing the
		// the flush channel.
	}
}

// Close the connection, and do not panic. Subsequent calls to Close return nil.
func (rs *RedisSubscription) Close() (err error) {
	if rs.closeRequested.Set(true) {
		err = rs.rps.Close()
	}
	return
}

// toggle
type toggle struct {
	state bool
	mu    sync.Mutex
}

func (t *toggle) Set(v bool) (previous bool) {
	t.mu.Lock()
	previous = t.state
	t.state = v
	t.mu.Unlock()
	return
}
func (t *toggle) Get() (v bool) {
	t.mu.Lock()
	v = t.state
	t.mu.Unlock()
	return
}
