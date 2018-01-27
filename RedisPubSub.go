package pubsub

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

//
type RedisPubSub struct {
	redisSub *RedisSubscription
	pubSub   *PubSub
}

// NewRedisPubSub creates
func NewRedisPubSub(conn redis.Conn) *RedisPubSub {
	pubSub := NewPubSub()

	redisSub := NewRedisSubscription(conn, func(channelName string, data []byte) {
		pubSub.Publish(channelName, data)
	})

	go func() {
		for {
			time.Sleep(time.Millisecond * 10)
			redisSub.Flush()
		}
	}()

	return &RedisPubSub{
		pubSub:   pubSub,
		redisSub: redisSub,
	}
}

func (rps *RedisPubSub) Update(agent Agent, add []string, rem []string) {
	var pendingAdd, pendingRem []string

	if rem != nil {
		pendingRem = make([]string, 0, len(rem))
		for _, channelName := range rem {
			changed := rps.pubSub.Unsubscribe(agent, channelName)
			if changed {
				pendingRem = append(pendingRem, channelName)
			}
		}
	}

	if add != nil {
		pendingAdd = make([]string, 0, len(add))
		for _, channelName := range add {
			changed := rps.pubSub.Subscribe(agent, channelName)
			if changed {
				pendingAdd = append(pendingAdd, channelName)
			}
		}
	}

	rps.redisSub.Unsubscribe(pendingRem...)
	rps.redisSub.Subscribe(pendingAdd...)
}
