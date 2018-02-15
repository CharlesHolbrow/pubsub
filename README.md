# PubSub

Concurrency safe multiplexer for redis publish subscribe.

Designed to "fan out" redis pubsub subscriptions to many clients.

Hello World:

```go
package main

import (
  "fmt"
  "time"

  "github.com/CharlesHolbrow/pubsub"
  "github.com/garyburd/redigo/redis"
)

// agent implements pubsub.Agent
type agent struct {
  id string
}

func (c agent) ID() string {
  return c.id
}

func (c agent) Receive(channel string, data []byte) error {
  fmt.Printf("agent %s received: (%s) %s\n", c.id, channel, data)
  // if an
  return nil
}

func main() {

  // Create a redigo subscription
  conn, err := redis.Dial("tcp", ":6379")
  if err != nil {
    panic("Error dialing redis: " + err.Error())
  }
  defer conn.Close()

  // Create some agents
  c1 := agent{id: "agent1"}
  c2 := agent{id: "agent2"}

  // Subscribe to Redis. This converts the supplied connection to pubsub mode,
  // and subscribed that redis channel to `ch1`, `ch2`, and `ch3`
  fmt.Println("Subscribing agents")
  rSub := pubsub.NewRedisAgents(conn)
  rSub.Update(c1, []string{"ch1", "ch2"}, nil)
  rSub.Update(c2, []string{"ch1", "ch3"}, nil)
  time.Sleep(20 * time.Second)

  // After there are no agents subscribed to `ch1`, the underlying redis
  // connection will be also be unsubscribed from the `ch1` redis channel.
  fmt.Println("Unsubscribe both channels from ch1")
  rSub.Update(c1, nil, []string{"ch1"})
  rSub.Update(c2, nil, []string{"ch1"})
  time.Sleep(360 * time.Second)
}
```
