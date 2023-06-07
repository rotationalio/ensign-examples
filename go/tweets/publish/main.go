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
				e := &ensign.Event{
					Mimetype: mimetype.ApplicationJSON,
					Type: &api.Type{
						Name:         "tweet",
						MajorVersion: 1,
						MinorVersion: 0,
						PatchVersion: 0,
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

				// On publish, the client checks to see if it has an open publish stream created
				// and if it doesn't it opens a stream to the correct Ensign node.
				// Topic alias also works
				client.Publish(topicID, e)

				fmt.Printf("published tweet with ID: %s\n", tweet.ID)
			}

			if len(rep.Raw.Tweets) > 0 {
				sinceID = rep.Raw.Tweets[0].ID
			}
		}
	}
}
