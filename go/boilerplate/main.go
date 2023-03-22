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

// Hey there! Here's where you write your code
// This is the nickname of your topic, it will get mapped to an ID that actually gets used by Ensign
const MyCoolEnsignTopic = "otters-are-the-best"

// Hey there! Here's where you write your code
// Create a custom struct if needed to unmarshal data you get back from your specific streaming data source
type YourCustomStruct struct {
}

// Consume is a helper function that takes as input a event chan that gets created by calling sub.Subscribe()
// and ranges over any events that it receives on the chan, unmarshals them, and prints them out
func Consume(events <-chan *api.Event) {
	for e := range events {
		customStruct := &YourCustomStruct{}
		if err := json.Unmarshal(e.Data, &customStruct); err != nil {
			panic("unable to unmarshal event: " + err.Error())
		}
		fmt.Printf("message received! \n %v", customStruct)
	}
}

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

	// Check to see if topic exists, if it does then the variable exists will be True
	exists, err := client.TopicExists(context.Background(), MyCoolEnsignTopic)
	if err != nil {
		panic(fmt.Errorf("unable to check topic existence: %s", err))
	}

	var topicID string
	// If the topic does not exist, create it using the CreateTopic method
	if !exists {
		if topicID, err = client.CreateTopic(context.Background(), MyCoolEnsignTopic); err != nil {
			panic(fmt.Errorf("unable to create topic: %s", err))
		}
	} else {
		// The topic does exist, but we need to figure out what the Topic ID is, so we need
		// to query the ListTopics method to get back a list of all the topic nickname : topicID mappings
		topics, err := client.ListTopics(context.Background())
		if err != nil {
			panic(fmt.Errorf("unable to retrieve project topics: %s", err))
		}

		// Now iterate over that mapping to get the topic we want
		for _, topic := range topics {
			if topic.Name == MyCoolEnsignTopic {
				var topicULID ulid.ULID
				if err = topicULID.UnmarshalBinary(topic.Id); err != nil {
					panic(fmt.Errorf("unable to retrieve requested topic: %s", err))
				}
				topicID = topicULID.String()
			}
		}
	}

	// Hey there! Here's where you write your code
	// to get data from your streaming source!

	// Create the Publisher on the client - we should publish each event to the same publisher
	pub, err := client.Publish(context.Background())
	if err != nil {
		panic("unable to create published from client: " + err.Error())
	}

	// Create a subscriber  - the same subscriber should be consuming each event that comes down the pipe
	sub, err := client.Subscribe(context.Background(), topicID)
	if err != nil {
		fmt.Printf("could not create subscriber: %s", err)
	}

	// Create event stream on the Subscriber we just made; this event channel is going to get passed into the Announce method
	var events <-chan *api.Event
	if events, err = sub.Subscribe(); err != nil {
		panic("failed to create subscribe stream: " + err.Error())
	}

	// Loop over each response that is returned by your streaming source, publish it to the topicID, have the subscriber consume to the events channel
	for {
		myPacket := &YourCustomStruct{}

		// Hey there! Here's where you write your code
		// Unmarshal each response you get from your streaming source into your custom struct

		// Put that unmarshaled data into an Ensign Event struct
		e := &api.Event{
			Mimetype: mimetype.ApplicationJSON,
			Type: &api.Type{
				Name:    "Generic",
				Version: 1,
			},
		}

		// Remarshal the Ensign Event so it's ready to publish!
		if e.Data, err = json.Marshal(myPacket); err != nil {
			panic("could not marshal data to JSON: " + err.Error())
		}

		// Publish the newly received tick event to the Topic
		fmt.Printf("Publishing to topic id: %s\n", topicID)
		time.Sleep(1 * time.Second)
		pub.Publish(topicID, e)

		// Goroutine to check the events channel to ensure that subscriber is getting all the events!
		time.Sleep(1 * time.Second)
		go Consume(events)
	}
}
