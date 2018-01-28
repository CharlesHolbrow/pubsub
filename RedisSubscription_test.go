package pubsub

import (
	"fmt"
	"testing"

	"github.com/garyburd/redigo/redis"
)

type tclient struct {
	id  string // return this string on ID()
	err error  // Return this error on Send()
}

func (c *tclient) Receive(message []byte) error {
	fmt.Printf("client %s Received: %s\n", c.id, message)
	return c.err
}
func (c *tclient) ID() string {
	return c.id
}

func createConn() redis.Conn {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic("failed to dial redis: " + err.Error()) // handle error
	}
	return c
}

func Test_RedisSubscription(t *testing.T) {
	c1 := &tclient{"c1", nil}
	c2 := &tclient{"c2", nil}

	conn := createConn()
	defer conn.Close()

	conn2 := createConn()
	defer conn2.Close()

	rps := NewRedisAgents(conn)
	fmt.Println("Before Update")
	rps.Update(c1, []string{"hello", "world"}, nil)
	fmt.Println("Middle")
	rps.Update(c2, []string{"hi", "world"}, nil)
	fmt.Println("After")

	r, e := conn2.Do("PUBLISH", "world", "This is a message!!")
	fmt.Printf("Result: %s\tErr:%v\n", r, e)

	// time.Sleep(time.Second)
	// t.Error("Should Fail!")
}
