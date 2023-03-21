package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
	mimetype "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

type MessageInABottle struct {
	Sender    string `json:"sender,omitempty"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

const CocoaBeans = "chocolate-covered-espresso-beans"

func main() {
	// Create Ensign Client
	client, err := ensign.New(&ensign.Options{
		ClientID:     os.Getenv("ENSIGN_CLIENT_ID"),
		ClientSecret: os.Getenv("ENSIGN_CLIENT_SECRET"),
		// AuthURL:      "https://auth.ensign.world", // uncomment if you are in staging
		// Endpoint:     "staging.ensign.world:443",  // uncomment if you are in staging
	})
	if err != nil {
		panic(fmt.Errorf("could not create client: %s", err))
	}

	// Check to see if topic exists and create it if not
	exists, err := client.TopicExists(context.Background(), CocoaBeans)
	if err != nil {
		panic(fmt.Errorf("unable to check topic existence: %s", err))
	}

	var topicID string
	if !exists {
		if topicID, err = client.CreateTopic(context.Background(), CocoaBeans); err != nil {
			panic(fmt.Errorf("unable to create topic: %s", err))
		}
	} else {
		topics, err := client.ListTopics(context.Background())
		if err != nil {
			panic(fmt.Errorf("unable to retrieve project topics: %s", err))
		}

		for _, topic := range topics {
			fmt.Println(topic)
			if topic.Name == CocoaBeans {
				var topicULID ulid.ULID
				if err = topicULID.UnmarshalBinary(topic.Id); err != nil {
					panic(fmt.Errorf("unable to retrieve requested topic: %s", err))
				}
				topicID = topicULID.String()
			}
		}
	}

	// Prep event data and marshal for transmission
	data := MessageInABottle{
		Sender:    "Enson the Sea Otter",
		Timestamp: time.Now().String(),
		Message:   "You're looking smart today!",
	}
	e := &api.Event{
		Mimetype: mimetype.ApplicationJSON,
		Type: &api.Type{
			Name:    "Generic",
			Version: 1,
		},
	}
	e.Data, _ = json.Marshal(data)

	// Create Ensign Publisher
	pub, err := client.Publish(context.Background())
	if err != nil {
		panic(fmt.Errorf("could not create publisher: %s", err))
	}

	// Create a downstream consumer for the event stream
	sub, err := client.Subscribe(context.Background(), topicID)
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}

	var events <-chan *api.Event
	if events, err = sub.Subscribe(); err != nil {
		panic("failed to create subscribe stream: " + err.Error())
	}

	// Publish the message in a bottle
	pub.Publish(topicID, e)

	// Wait for events to come from the subscriber.
	for msg := range events {
		var m MessageInABottle
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			panic(fmt.Errorf("failed to unmarshal message: %s", err))
		}
		fmt.Printf("At %s,\n%s\nsent you the following message...\n'%s'\n", m.Timestamp, m.Sender, m.Message)
		return
	}
}
