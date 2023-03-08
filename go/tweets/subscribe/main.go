package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rotationalio/ensign-examples/go/tweets/schemas"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
)

func main() {
	var (
		err    error
		client *ensign.Client
	)

	// ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment variables must be set
	if client, err = ensign.New(&ensign.Options{
		Endpoint: "flagship.rotational.dev:443",
	}); err != nil {
		panic("failed to create Ensign client: " + err.Error())
	}

	// Create a subscriber from the client
	var sub ensign.Subscriber
	if sub, err = client.Subscribe(context.Background()); err != nil {
		panic("failed to create subscriber from client: " + err.Error())
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
