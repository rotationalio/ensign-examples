package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type SteamApps struct {
	AppList struct {
		Apps []SteamApp
	}
}

type SteamApp struct {
	AppId uint64
	Name  string
}

type AppReviews struct {
	Success      int
	QuerySummary QuerySummary
	Reviews      []Review
}

type QuerySummary struct {
	NumberReviews          int
	ReviewScore            int
	ReviewScoreDescription string
	TotalPositive          int
	TotalNegative          int
	TotalReviews           int
}

type Review struct {
	ID                string
	Author            Author
	Language          string
	Review            string
	TimeCreated       int64
	TimeUpdated       int64
	VotedUp           bool
	VotesUp           int
	VotesDown         int
	VotesFunny        int
	WeightedVoteScore string
	CommentCount      json.RawMessage
	SteamPurchase     bool
	ReceivedForFree   bool
	EarlyAccess       bool
}

type Author struct {
	UserID               string
	NumberGamesOwned     int
	NumberReviews        int
	PlayTimeForever      int
	PlaytimeLastTwoWeeks int
	LastPlayed           int64
}

func main() {
	var err error
	var response *http.Response
	if response, err = http.Get("https://api.steampowered.com/ISteamApps/GetAppList/v2/"); err != nil {
		fmt.Println(err)
		return
	}
	defer response.Body.Close()

	var apps SteamApps
	if response.StatusCode != 200 {
		fmt.Println("status code", response.StatusCode)
		return
	}
	json.NewDecoder(response.Body).Decode(&apps)

	var reviews AppReviews
	url := "https://store.steampowered.com/appreviews/413150?json=1'"
	if response, err = http.Get(url); err != nil {
		fmt.Println(err)
		return
	}
	json.NewDecoder(response.Body).Decode(&reviews)

	fmt.Println(len(reviews.Reviews))
	fmt.Println(reviews.Reviews[0].Review)
}
