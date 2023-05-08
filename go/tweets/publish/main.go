package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"context"

	"github.com/oklog/ulid/v2"
	twitter "github.com/g8rswimmer/go-twitter/v2"
	"github.com/rotationalio/ensign-examples/go/tweets/schemas"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
	mimetype "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

const DistSysTweets = "distsys-tweets"

type authorize struct {
	Token string
}

func (a authorize) Add(req *http.Request) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", a.Token))
}

func main() {
	var (
		err   error
		token string
	)

	if token = os.Getenv("TWITTER_API_BEARER_TOKEN"); token == "" {
		panic("TWITTER_API_BEARER_TOKEN environment variable is required")
	}

	query := flag.String("query", "distributed systems", "Twitter search query")
	flag.Parse()

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

	// Create Ensign Publisher
	pub, err := client.Publish(context.Background())
	if err != nil {
		panic(fmt.Errorf("could not create publisher: %s", err))
	}
	defer pub.Close()

	tweets := &twitter.Client{
		Authorizer: authorize{
			Token: token,
		},
		Client: http.DefaultClient,
		Host:   "https://api.twitter.com",
	}

	ticker := time.NewTicker(10 * time.Second)
	sinceID := ""
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("searching for tweets using query %q\n", *query)
			opts := twitter.TweetRecentSearchOpts{
				SortOrder: twitter.TweetSearchSortOrderRecency,
				SinceID:   sinceID,
			}

			var rep *twitter.TweetRecentSearchResponse
			if rep, err = tweets.TweetRecentSearch(ctx, *query, opts); err != nil {
				panic(err)
			}

			for _, errs := range rep.Raw.Errors {
				fmt.Printf("Error: %s\n", errs.Detail)
			}

			for _, tweet := range rep.Raw.Tweets {
				e := &api.Event{
					TopicId:  "tweets",
					Mimetype: mimetype.ApplicationJSON,
					Type: &api.Type{
						Name:    "tweet",
						Version: 1,
					},
				}

				tweetObj := &schemas.Tweet{
					ID:        tweet.ID,
					Text:      tweet.Text,
					CreatedAt: tweet.CreatedAt,
				}
				if e.Data, err = json.Marshal(tweetObj); err != nil {
					panic("could not marshal tweet to JSON: " + err.Error())
				}

				// Publish the event to Ensign
				pub.Publish(topicID, e)

				// Check for errors
				// Note: This is asynchronous so the error might not correspond to the
				// most recently published event
				if err = pub.Err(); err != nil {
					panic("failed to publish event(s): " + err.Error())
				}

				fmt.Printf("published tweet with ID: %s\n", tweet.ID)
			}

			if len(rep.Raw.Tweets) > 0 {
				sinceID = rep.Raw.Tweets[0].ID
			}
		}
	}
}
