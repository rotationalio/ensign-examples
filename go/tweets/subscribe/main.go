package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rotationalio/ensign-examples/go/tweets/schemas"
	ensign "github.com/rotationalio/go-ensign"
)

const DistSysTweets = "distsys-tweets"

func main() {
	// Create Ensign Client
	client, err := ensign.New() // if your credentials are already in your bash profile, you don't have to pass anything into New()
	if err != nil {
		panic(fmt.Errorf("could not create client: %s", err))
	}

	// Check to see if topic exists and create it if not
	exists, err := client.TopicExists(context.Background(), DistSysTweets)
	if err != nil {
		panic(fmt.Errorf("unable to check topic existence: %s", err))
	}

	var topicID string
	if !exists {
		if topicID, err = client.CreateTopic(context.Background(), DistSysTweets); err != nil {
			panic(fmt.Errorf("unable to create topic: %s", err))
		}
	} else {
		// The topic does exist, but we need to figure out what the Topic ID is, so we need
		// to query the ListTopics method to get back a list of all the topic nickname : topicID mappings
		if topicID, err = client.TopicID(context.Background(), DistSysTweets); err != nil {
			panic(fmt.Errorf("unable to get id for topic: %s", err))
		}
	}

	// Create a subscriber from the client
	// Create a downstream consumer for the event stream
	sub, err := client.Subscribe(topicID)
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}

	// Events are processed as they show up on the channel
	for event := range sub.C {
		tweet := &schemas.Tweet{}
		if err = json.Unmarshal(event.Data, tweet); err != nil {
			panic("failed to unmarshal event: " + err.Error())
		}

		fmt.Printf("received tweet %s\n", tweet.ID)
		fmt.Println(tweet.Text)
		fmt.Println()
	}
}
