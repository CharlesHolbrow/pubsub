package pubsub

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisAgents allows many Agents to subscribe to redis PUB/SUB channels. Agent
// subscriptions are multiplexed to a single redis connection.
type RedisAgents struct {
	redisSubscription *RedisSubscription
	agentPubSub       *PubSub
}

// NewRedisAgents creates a new RedisAgents instance. The redis connection will
// be passed to pubsub.NewRedisSubscription. See pubsub.RedisSubscription to
// verify how the connection may (or may not) be closed in the event of an
// error.
func NewRedisAgents(conn redis.Conn) *RedisAgents {
	agentPubSub := NewPubSub()
	redisSubscription := NewRedisSubscription(conn, func(channelName string, data []byte) {
		agentPubSub.Publish(channelName, data)
	})

	go func() {
		for {
			// Note that if requests by a single agent come in too fast, they
			// will accumulate, and we will quickly run out of memory.
			time.Sleep(time.Millisecond * 6)
			redisSubscription.Flush()
		}
	}()

	return &RedisAgents{
		agentPubSub:       agentPubSub,
		redisSubscription: redisSubscription,
	}
}

// Update an Agent's subscriptions by adding or removing subscription keys. This
// will update the redisSubscription iff needed.
//
// It is the caller's responsibility to ensure that no two Agents have the same
// ID.
func (rps *RedisAgents) Update(agent Agent, add []string, rem []string) {
	var pendingAdd, pendingRem []string

	if len(rem) > 0 {
		pendingRem = make([]string, 0, len(rem))
		for _, channelName := range rem {
			changed := rps.agentPubSub.Unsubscribe(agent, channelName)
			if changed {
				pendingRem = append(pendingRem, channelName)
			}
		}
	}

	if len(add) > 0 {
		pendingAdd = make([]string, 0, len(add))
		for _, channelName := range add {
			changed := rps.agentPubSub.Subscribe(agent, channelName)
			if changed {
				pendingAdd = append(pendingAdd, channelName)
			}
		}
	}

	// Update the redis subscription if needed.
	if len(pendingRem) > 0 {
		rps.redisSubscription.Unsubscribe(pendingRem...)
	}
	if len(pendingAdd) > 0 {
		// .Subscribe will not return unill the next call to .Flush
		rps.redisSubscription.Subscribe(pendingAdd...)
	}
}
