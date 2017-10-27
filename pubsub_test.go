package pubsub

import (
	"fmt"
	"sync"
	"testing"
)

func TestPubSub_subscribe(t *testing.T) {
	type fields struct {
		channels    map[string]pubSubChannel
		subscribers map[string]pubSubSubscriber
		lock        sync.RWMutex
	}
	type args struct {
		key        string
		subscriber Subscriber
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantChanged bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := &PubSub{
				channels:    tt.fields.channels,
				subscribers: tt.fields.subscribers,
				lock:        tt.fields.lock,
			}
			if gotChanged := ps.subscribe(tt.args.key, tt.args.subscriber); gotChanged != tt.wantChanged {
				t.Errorf("PubSub.subscribe() = %v, want %v", gotChanged, tt.wantChanged)
			}
		})
	}
}

type client struct {
	id string
}

func (c *client) Send(message string) error {
	fmt.Printf("client %s sending: %s\n", c.id, message)
	return nil
}
func (c *client) ID() string {
	return c.id
}

func TestPubSub_Subscribe(t *testing.T) {
	ps := NewPubSub()
	c1 := &client{"c1"}
	// c2 := &client{"c2"}

	ps.Subscribe("0|0", c1)
	channel, ok := ps.channels["0|0"]

	if !ok {
		t.Error("expected Subscribe() to create channel")
	}

	if channel["c1"] != c1 {
		t.Errorf("expected channel %s to be inserted, but found %s", *c1, channel["c1"])
	}

}
