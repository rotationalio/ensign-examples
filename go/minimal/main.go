package main

import (
	"context"
	"encoding/json"
	"fmt"
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

const CocoaBeans = "chocolate-covered-espresso-beans"

func main() {
	// Create Ensign Client
	client, err := ensign.New() // if your credentials are already in your bash profile, you don't have to pass anything into New()
	// client, err := ensign.New(ensign.WithCredentials("YOUR CLIENT ID HERE!", "YOUR CLIENT SECRET HERE!"))
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
		// The topic does exist, but we need to figure out what the Topic ID is, so we need
		// to query the ListTopics method to get back a list of all the topic nickname : topicID mappings
		if topicID, err = client.TopicID(context.Background(), CocoaBeans); err != nil {
			panic(fmt.Errorf("unable to get id for topic: %s", err))
		}
	}

	// Prep event data and marshal for transmission
	data := MessageInABottle{
		Sender:    "Enson the Sea Otter",
		Timestamp: time.Now().String(),
		Message:   "You're looking smart today!",
	}
	// Put that unmarshaled data into an Ensign Event struct
	e := &ensign.Event{
		Mimetype: mimetype.ApplicationJSON,
		Type: &api.Type{
			Name:         "Generic",
			MajorVersion: 1,
			MinorVersion: 0,
			PatchVersion: 0,
		},
	}
	// Remarshal the Ensign Event so it's ready to publish!
	if e.Data, err = json.Marshal(data); err != nil {
		panic("could not marshal data to JSON: " + err.Error())
	}

	// Create a subscriber  - the same subscriber should be consuming each event that comes down the pipe
	sub, err := client.Subscribe(topicID) // topic alias also works
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}

	fmt.Printf("Publishing to topic id: %s\n", topicID)
	time.Sleep(1 * time.Second)

	// Publish the message in a bottle after waiting for a second
	// On publish, the client checks to see if it has an open publish stream created
	// and if it doesn't it opens a stream to the correct Ensign node.
	// Topic alias also works
	client.Publish(topicID, e)
	// client.Publish(topicID, e, a, f, h) // Can publish a couple events if you want!
	// client.Publish(differentTopicId, e) // or, if you Publish to a second, valid topicID, the Ensign client will create another new Publisher!

	// Goroutine to check the events channel to ensure that subscriber is getting all the events!
	time.Sleep(1 * time.Second)
	for msg := range sub.C {
		var m MessageInABottle
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			panic(fmt.Errorf("failed to unmarshal message: %s", err))
		}
		fmt.Printf("At %s,\n%s\nsent you the following message...\n'%s'\n", m.Timestamp, m.Sender, m.Message)
		return
	}
}
