package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/cdipaolo/sentiment"

	post "github.com/rotationalio/baleen/events"
	"github.com/rotationalio/ensign-examples/go/nlp/parse"
	api "github.com/rotationalio/ensign/pkg/api/v1beta1"
	ensign "github.com/rotationalio/ensign/sdks/go"
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

	fmt.Printf("Ensign connection established at %s\n", time.Now().String())

	// Create a subscriber from the client
	var sub ensign.Subscriber
	if sub, err = client.Subscribe(context.Background()); err != nil {
		panic("failed to create subscriber from client: " + err.Error())
	}
	fmt.Printf("subscriber created at %s\n", time.Now().String())

	defer sub.Close()

	// Load the sentiment model
	var model sentiment.Models
	if model, err = sentiment.Restore(); err != nil {
		panic("failed to load sentiment model" + err.Error())
	}

	// Create files to store entity data
	f, err := os.Create("entities.csv")
	if err != nil {
		panic("failed creating file" + err.Error())
	}
	defer f.Close()

	// Create the event stream as a channel
	var events <-chan *api.Event
	if events, err = sub.Subscribe(); err != nil {
		panic("failed to create subscribe stream: " + err.Error())
	}

	// Events are processed as they show up on the channel
	for event := range events {
		if event.Type.Name == "FeedItem" {
			fmt.Printf("FeedItem detected at %s\n", time.Now().String())
		}
		if event.Type.Name == "Document" {
			fmt.Printf("document detected at %s\n", time.Now().String())

			doc := &post.Document{}
			if _, err = doc.UnmarshalMsg(event.Data); err != nil {
				panic("failed to unmarshal event: " + err.Error())
			}
			fmt.Println("received document")
			var entities map[string]string
			var avgSentiment float32
			if entities, avgSentiment, err = parse.ParseResponse(doc, model); err != nil {
				fmt.Println("failed to extract entities from response:", err)
			}
			writer := csv.NewWriter(f)
			defer writer.Flush()
			for ent, tag := range entities {
				// Construct the rows; each row has:
				// Extracted entity, entity type, article title, article date, article link, average sentiment)
				var row []string

				row = append(row, ent, tag, doc.Title, doc.FetchedAt.String(), doc.Link, fmt.Sprintf("%f", avgSentiment))

				// Write the rows
				if err = writer.Write(row); err != nil {
					fmt.Println("failed to write extracted entities to csv:", err)
				}

			}
			fmt.Println("stored extracted entities to csv")

		} else {
			fmt.Printf("no document events for now (%s)...\n", time.Now().String())
		}
	}
}
