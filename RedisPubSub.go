package pubsub

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisAgents allows many agents to subscribe to redis PUB/SUB channels. Agent
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
	redisSub := NewRedisSubscription(conn, func(channelName string, data []byte) {
		agentPubSub.Publish(channelName, data)
	})

	go func() {
		for {
			time.Sleep(time.Millisecond * 6)
			redisSub.Flush()
		}
	}()

	return &RedisAgents{
		agentPubSub:       agentPubSub,
		redisSubscription: redisSub,
	}
}

//
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

	rps.redisSubscription.Unsubscribe(pendingRem...)
	rps.redisSubscription.Subscribe(pendingAdd...)
}
