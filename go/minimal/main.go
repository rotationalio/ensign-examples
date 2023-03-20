package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
	mimetype "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

type MessageInABottle struct {
	Sender    string `json:"sender,omitempty"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

func main() {
	client, err := ensign.New(&ensign.Options{
		ClientID:     os.Getenv("ENSIGN_CLIENT_ID"),
		ClientSecret: os.Getenv("ENSIGN_CLIENT_SECRET"),
	})
	if err != nil {
		panic(fmt.Errorf("could not create client: %s", err))
	}

	pub, err := client.Publish(context.Background())
	if err != nil {
		panic(fmt.Errorf("could not create publisher: %s", err))
	}

	data := MessageInABottle{
		Sender:    "Enson the Sea Otter",
		Timestamp: time.Now().String(),
		Message:   "You're looking smart today!",
	}

	e := &api.Event{
		TopicId:  "chocolate-covered-espresso-beans",
		Mimetype: mimetype.ApplicationJSON,
		Type: &api.Type{
			Name:    "Generic",
			Version: 1,
		},
	}

	e.Data, _ = json.Marshal(data)
	pub.Publish(e)

	sub, err := client.Subscribe(context.Background())
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}

	var events <-chan *api.Event
	if events, err = sub.Subscribe(); err != nil {
		panic("failed to create subscribe stream: " + err.Error())
	}

	for msg := range events {
		var m MessageInABottle
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			panic(fmt.Errorf("failed to unmarshal message: %s", err))
		}
		fmt.Printf("At %s,\n%s\nsent you the following message...\n'%s'\n", m.Timestamp, m.Sender, m.Message)
		return
	}
}
