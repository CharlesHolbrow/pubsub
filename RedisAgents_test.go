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

func TestRedisAgents_Update(t *testing.T) {
	totalReceived := &counter{name: "Receiver"}
	clients := nNewClients(2000, totalReceived)
	clientCount := len(clients)
	clientTime := time.Duration(clientCount)

	fmt.Printf("Created %d new clients\n", len(clients))

	// Create two redis connections, one for subscribing, one for publishing.
	conn := createConn() // Will be closed by RedisSubscription
	redisAgents := NewRedisAgents(conn)

	publisher := createConn()
	defer publisher.Close()

	// subscribe every client to it's name
	group := sync.WaitGroup{}
	group.Add(len(clients))

	start := time.Now()
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
	fmt.Printf("Added %d clients with %v per client.\n", clientCount, time.Since(start)/clientTime)

	// Try publishing to all clients
	fmt.Println("Publishing to 'all'...")
	totalReceived.StartCountingTo(clientCount)
	publish(publisher, "all", "everyone!")
	totalReceived.Wait()

	// publish to each client
	fmt.Println("Publishing to individual channels...")
	totalReceived.StartCountingTo(clientCount)
	for _, client := range clients {
		publish(publisher, client.ID(), "To ClientID")
	}
	totalReceived.Wait()

	subs, err := redis.Strings(publisher.Do("PUBSUB", "CHANNELS"))
	fmt.Printf("Subscribed to %d channels (%v)\n", len(subs), err)

	// Unsubscribe
	for _, client := range clients {
		go func(client *tclient) {
			rem := make([]string, 1)
			add := make([]string, 1)
			rem[0] = client.ID()
			add[0] = "all2"
			redisAgents.Update(client, add, rem)
		}(client)
	}

	subs, err = redis.Strings(publisher.Do("PUBSUB", "CHANNELS"))
	fmt.Printf("Subscribed to %d channels. error(%v)\n", len(subs), err)

	time.Sleep(time.Second)

	subs, err = redis.Strings(publisher.Do("PUBSUB", "CHANNELS"))
	fmt.Printf("Subscribed to %d channels error(%v)\n", len(subs), err)
}

// Create many tclients that all share a counter
func nNewClients(n int, c *counter) []*tclient {
	clients := make([]*tclient, n)
	for i := range clients {
		clients[i] = &tclient{
			id:            fmt.Sprintf("tc%d", i),
			data:          make([]string, 0, 2),
			totalReceived: c,
		}
	}
	return clients
}

// tclient is an Agent used for testing.
type tclient struct {
	id  string // return this string on ID()
	err error  // Return this error on Receive()

	// store all received messages
	sync.Mutex
	data []string

	// counter has its own mutex
	totalReceived *counter
}

// Push a string onto the internal data storage. Safe for concurrent calls.
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

func (c *tclient) Receive(channel string, message []byte) error {
	c.Push(string(message))
	c.totalReceived.Inc()
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

func (c *tclient) SetError(err error) {
	c.Lock()
	c.err = err
	c.Unlock()
}

// Counter is a minimal benchmarking tool for benchmarking asychronous events.
// There is probably a more idomatic (and accurate) way to do this.
type counter struct {
	sync.Mutex
	value int
	limit int
	name  string
	done  chan struct{}
	start time.Time
}

func (c *counter) Inc() {
	c.Lock()
	c.value++

	if c.value == c.limit {
		duration := time.Since(c.start)
		fmt.Printf("%s counter reached %d. Averaged %v per tick, total: %v\n", c.name, c.limit, duration/time.Duration(c.limit), duration)
		close(c.done)
		c.done = nil
	}
	c.Unlock()
}

func (c *counter) Value() (result int) {
	c.Lock()
	result = c.value
	c.Unlock()
	return
}

func (c *counter) StartCountingTo(limit int) {
	c.Lock()
	c.limit = limit
	c.value = 0
	// replace c.done. If it exists, it will likely never complete.
	c.done = make(chan struct{})
	c.start = time.Now()
	c.Unlock()
}

func (c *counter) Wait() {
	c.Lock()
	done := c.done
	c.Unlock()
	if done != nil {
		<-done
	}
}
