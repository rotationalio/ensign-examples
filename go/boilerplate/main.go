package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rotationalio/go-ensign"
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
func Consume(events <-chan *ensign.Event) {
	for e := range events {
		customStruct := &YourCustomStruct{}
		if err := json.Unmarshal(e.Data, &customStruct); err != nil {
			panic("unable to unmarshal event: " + err.Error())
		}
		fmt.Printf("message received! \n %v", customStruct)

		// Let ensign know the event has been processed correctly so you get the next
		// event in the topic!
		e.Ack()
	}
}

func main() {

	// Create Ensign Client
	client, err := ensign.New() // if your credentials are already in your bash profile, you don't have to pass anything into New()
	// client, err := ensign.New(ensign.WithCredentials("YOUR CLIENT ID HERE!", "YOUR CLIENT SECRET HERE!"))
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
		if topicID, err = client.TopicID(context.Background(), MyCoolEnsignTopic); err != nil {
			panic(fmt.Errorf("unable to get id for topic: %s", err))
		}
	}

	// Hey there! Here's where you write your code
	// to get data from your streaming source!

	// Create a subscriber  - the same subscriber should be consuming each event that comes down the pipe
	sub, err := client.Subscribe(topicID) // topic alias also works
	if err != nil {
		fmt.Printf("could not create subscriber: %s", err)
	}

	// Loop over each response that is returned by your streaming source, publish it to the topicID, have the subscriber consume to the events channel
	for {
		myPacket := &YourCustomStruct{}

		// Hey there! Here's where you write your code
		// Unmarshal each response you get from your streaming source into your custom struct

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
		if e.Data, err = json.Marshal(myPacket); err != nil {
			panic("could not marshal data to JSON: " + err.Error())
		}

		// Publish the newly received tick event to the Topic
		fmt.Printf("Publishing to topic id: %s\n", topicID)
		time.Sleep(1 * time.Second)

		// On publish, the client checks to see if it has an open publish stream created
		// and if it doesn't it opens a stream to the correct Ensign node.
		// Topic alias also works
		client.Publish(topicID, e)
		// client.Publish(topicID, e, a, f, h) // Can publish a couple events if you want!
		// client.Publish(differentTopicId, e) // or, if you Publish to a second, valid topicID, the Ensign client will create another new Publisher!

		// Goroutine to check the events channel to ensure that subscriber is getting all the events!
		time.Sleep(1 * time.Second)
		go Consume(sub.C)
	}
}
