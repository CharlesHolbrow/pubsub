package pubsub

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func createConn() redis.Conn {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic("failed to dial redis: " + err.Error()) // handle error
	}
	return c
}

func publish(c redis.Conn, channelName, message string) {
	if _, e := c.Do("PUBLISH", channelName, message); e != nil {
		panic("Error Trying to publish: " + e.Error())
	}
}

func Test_RedisSubscription(t *testing.T) {
	clients := nNewClients(5000)
	fmt.Printf("Created %d new clients\n", len(clients))

	// Create two redis connections, one for subscribing, on
	conn := createConn()
	defer conn.Close()
	publisher := createConn()
	defer publisher.Close()

	redisAgents := NewRedisAgents(conn)

	// subscribe every client to it's name
	group := sync.WaitGroup{}
	group.Add(len(clients))

	for _, client := range clients {
		go func(client *tclient) {
			add := make([]string, 2)
			add[0] = "all"
			add[1] = client.ID()
			redisAgents.Update(client, add, nil)
			group.Done()
		}(client)
	}
	group.Wait()

	fmt.Printf("Added %d clients. publishing to all...\n", len(clients))
	publish(publisher, "all", "everyone!")
	fmt.Println("Done publishing!")

	time.Sleep(time.Millisecond * 500)
}

// Create many tclients
func nNewClients(n int) []*tclient {
	clients := make([]*tclient, n)
	for i := range clients {
		clients[i] = &tclient{
			id:   fmt.Sprintf("tc%d", i),
			data: make([]string, 0, 2),
		}
	}
	return clients
}

// tclient is an Agent used for testing.
type tclient struct {
	id  string // return this string on ID()
	err error  // Return this error on Send()

	// store all received messages
	sync.Mutex
	data []string
}

func (c *tclient) Push(msg string) {
	c.Lock()
	if c.data == nil {
		c.data = make([]string, 0)
	}
	c.data = append(c.data, msg)
	c.Unlock()
}

func (c *tclient) String() string {
	c.Lock()
	defer c.Unlock()

	if c.data == nil {
		return "<empty>"
	}
	return strings.Join(c.data, ", ")
}

func (c *tclient) Receive(message []byte) error {
	c.Push(string(message))
	return c.err
}

func (c *tclient) Length() int {
	c.Lock()
	defer c.Unlock()
	return len(c.data)
}

func (c *tclient) ID() string {
	return c.id
}
