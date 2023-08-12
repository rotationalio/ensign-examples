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
	ensign "github.com/rotationalio/go-ensign"
)

// This is the nickname of the topic, which gets mapped to an ID that actually gets used by Ensign
const Baleen = "baleen-docs"

func main() {
	// Create Ensign Client
	client, err := ensign.New()
	if err != nil {
		panic(fmt.Errorf("could not create client: %s", err))
	}
	fmt.Printf("Ensign connection established at %s\n", time.Now().String())

	// Check to see if topic exists and create it if not
	exists, err := client.TopicExists(context.Background(), Baleen)
	if err != nil {
		panic(fmt.Errorf("unable to check topic existence: %s", err))
	}

	if !exists {
		if _, err = client.CreateTopic(context.Background(), Baleen); err != nil {
			panic(fmt.Errorf("unable to create topic: %s", err))
		}
	}

	// Create a downstream consumer for the event stream
	sub, err := client.Subscribe(Baleen)
	if err != nil {
		panic(fmt.Errorf("could not create subscriber: %s", err))
	}
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

	// Events are processed as they show up on the channel
	for event := range sub.C {
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
