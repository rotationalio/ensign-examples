package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"context"

	twitter "github.com/g8rswimmer/go-twitter/v2"
	"github.com/rotationalio/ensign-examples/go/tweets/schemas"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"
	mimetype "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

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

	// ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment variables must be set
	var client *ensign.Client
	if client, err = ensign.New(&ensign.Options{
		Endpoint: "flagship.rotational.dev:443",
	}); err != nil {
		panic("failed to create Ensign client: " + err.Error())
	}

	// Create a publisher from the client
	var pub ensign.Publisher
	if pub, err = client.Publish(context.Background()); err != nil {
		panic("failed to create publisher from client: " + err.Error())
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
				pub.Publish(e)

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
