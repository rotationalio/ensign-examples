package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/oklog/ulid/v2"
	"github.com/rotationalio/ensign-examples/go/tweets/schemas"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
)

const DistSysTweets = "distsys-tweets"

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
		topics, err := client.ListTopics(context.Background())
		if err != nil {
			panic(fmt.Errorf("unable to retrieve project topics: %s", err))
		}

		for _, topic := range topics {
			if topic.Name == DistSysTweets {
				var topicULID ulid.ULID
				if err = topicULID.UnmarshalBinary(topic.Id); err != nil {
					panic(fmt.Errorf("unable to retrieve requested topic: %s", err))
				}
				topicID = topicULID.String()
			}
		}
	}

	// Create a subscriber from the client
	// Create a downstream consumer for the event stream
	sub, err := client.Subscribe(context.Background(), topicID)
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}
	defer sub.Close()

	// Create the event stream as a channel
	var events <-chan *api.Event
	if events, err = sub.Subscribe(); err != nil {
		panic("failed to create subscribe stream: " + err.Error())
	}

	// Events are processed as they show up on the channel
	for event := range events {
		tweet := &schemas.Tweet{}
		if err = json.Unmarshal(event.Data, tweet); err != nil {
			panic("failed to unmarshal event: " + err.Error())
		}

		fmt.Printf("received tweet %s\n", tweet.ID)
		fmt.Println(tweet.Text)
		fmt.Println()
	}
}
