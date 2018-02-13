package pubsub

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisAgents allows many Agents to subscribe to redis PUB/SUB channels. Agent
// subscriptions are multiplexed to a single redis connection.
type RedisAgents struct {
	redisSubscription *RedisSubscription
	agentPubSub       *PubSub

	flushLock sync.RWMutex
	flush     chan struct{}
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

	redisAgents := &RedisAgents{
		agentPubSub:       agentPubSub,
		redisSubscription: redisSubscription,
		flush:             make(chan struct{}),
	}

	go func() {
		for {
			// Note that if requests by a single agent come in too fast, they
			// will accumulate, and we will quickly run out of memory.
			time.Sleep(time.Millisecond * 1)
			redisAgents.Flush()
		}
	}()

	return redisAgents
}

// Update an Agent's subscriptions by adding or removing subscription keys.
// Blocks until the next call to Flush()
//
// It is the caller's responsibility to ensure that no two Agents have the same
// ID.
func (redisAgents *RedisAgents) Update(agent Agent, add []string, rem []string) {
	redisAgents.flushLock.RLock()
	redisAgents.agentPubSub.lock.Lock()

	if len(rem) > 0 {
		for _, channelName := range rem {
			redisAgents.agentPubSub.unsubscribe(agent, channelName)
		}
	}

	if len(add) > 0 {
		for _, channelName := range add {
			redisAgents.agentPubSub.subscribe(agent, channelName)
		}
	}
	redisAgents.agentPubSub.lock.Unlock()
	flush := redisAgents.flush
	redisAgents.flushLock.RUnlock()

	<-flush
}

// RemoveAgent removes an agent, and unsubscribes it from all channels. The
// agent will immediately stop receiveing messages. Any unused redis channels
// will by unsubscribed on the next Flush.
func (redisAgents *RedisAgents) RemoveAgent(agent Agent) error {
	return redisAgents.agentPubSub.RemoveAgent(agent)
}

// Flush Updates the redis subscription by adding and removing channels so the
// redis subscription matches the agent subscription.
//
// Calling Flush allows all waiting Update() calls to return
func (redisAgents *RedisAgents) Flush() {
	redisAgents.flushLock.Lock()
	redisAgents.agentPubSub.RemoveAllBadAgents()
	add, rem := redisAgents.agentPubSub.Flush()
	redisAgents.redisSubscription.Update(add, rem)

	flush := redisAgents.flush              // Get the current flush channel
	redisAgents.flush = make(chan struct{}) // Create a new flush channel
	redisAgents.flushLock.Unlock()
	close(flush)
}
