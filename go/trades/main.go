package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
	mimetype "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

// This is the nickname of the topic, it will get mapped to an ID that actually gets used by Ensign
const Trades = "trades"

// This represents the structure of an individual stock data point that comes back from the Finnhub API
type Data struct {
	Symbol     string   `json:"s"`
	Price      float64  `json:"p"`
	Timestamp  uint64   `json:"t"`
	Conditions []string `json:"c" omitempty:"true"`
}

// This represents the entire websocket response that comes back from a single call to the Finnhub Server
// Note that a single Response may contain many Data points
type Response struct {
	Type string `json:"type"`
	Data []Data `json:"data"`
}

// Announce is a helper function that takes as input a event chan that gets created by calling sub.Subscribe()
// and ranges over any events that it receives on the chan, unmarshals them, and prints them out
func Announce(events <-chan *ensign.Event) {
	for tick := range events {
		trades := &Response{}
		if err := json.Unmarshal(tick.Data, &trades); err != nil {
			panic("unable to unmarshal event: " + err.Error())
		}
		fmt.Println(trades)
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
	exists, err := client.TopicExists(context.Background(), Trades)
	if err != nil {
		panic(fmt.Errorf("unable to check topic existence: %s", err))
	}

	var topicID string
	// If the topic does not exist, create it using the CreateTopic method
	if !exists {
		if topicID, err = client.CreateTopic(context.Background(), Trades); err != nil {
			panic(fmt.Errorf("unable to create topic: %s", err))
		}
	} else {
		// The topic does exist, but we need to figure out what the Topic ID is, so we need
		// to query the ListTopics method to get back a list of all the topic nickname : topicID mappings
		if topicID, err = client.TopicID(context.Background(), Trades); err != nil {
			panic(fmt.Errorf("unable to get id for topic: %s", err))
		}

	}

	key := os.Getenv("FINNHUB_KEY")
	if key == "" {
		panic("Finnhub key is required: get one at https://finnhub.io/")
	}

	// Get trades from Finnhub - FYI this Dialer dials the "Trades" endpoint
	// see https://finnhub.io/docs/api/websocket-trades for more details
	finnhub_url := fmt.Sprint("wss://ws.finnhub.io?token=", key)
	w, _, err := websocket.DefaultDialer.Dial(finnhub_url, nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// The complete list of options is long! This is a short list, but no guarantee that all will be updated for every tick
	symbols := []string{"AAPL", "AMZN", "PCG", "SNAP"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		w.WriteMessage(websocket.TextMessage, msg)
	}

	// Create a subscriber  - the same subscriber should be consuming each event that comes down the pipe
	sub, err := client.Subscribe(topicID)
	if err != nil {
		fmt.Printf("could not create subscriber: %s", err)
	}

	// Loop over each response that is returned by the Finnhub websocket, publish it to the topicID, have the subscriber consume to the events channel
	for {
		// The Response struct is how we will boost the standard json marshalling library to know how to unpack and repackage Finnhub ticks
		msg := &Response{}
		err := w.ReadJSON(&msg)
		if err != nil {
			panic(err)
		}
		fmt.Println("Message from the websocket server is ", msg)

		e := &ensign.Event{
			Mimetype: mimetype.ApplicationJSON,
			Type: &api.Type{
				Name:         "Generic",
				MajorVersion: 1,
				MinorVersion: 0,
				PatchVersion: 0,
			},
		}

		if e.Data, err = json.Marshal(msg); err != nil {
			panic("could not marshal data to JSON: " + err.Error())
		}

		// Publish the newly received tick event to the Topic
		fmt.Printf("Publishing to topic id: %s\n", topicID)
		time.Sleep(1 * time.Second)
		client.Publish(topicID, e)

		// Goroutine to check the events channel to ensure that subscriber is getting all the ticks!
		time.Sleep(1 * time.Second)
		go Announce(sub.C)
	}
}
