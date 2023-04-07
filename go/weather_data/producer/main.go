package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/rotationalio/watermill-ensign/pkg/ensign"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func main() {
	// add a logger
	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the producer", watermill.LogFields{})

	//create the publisher
	publisher, err := ensign.NewPublisher(
		ensign.PublisherConfig{
			EnsureCreateTopic: true,
			Marshaler:         ensign.EventMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	//used to signal the publisher to stop publishing
	closeCh := make(chan struct{})

	go publishWeatherData(publisher, closeCh)

	// wait for SIGINT - this will end processing
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// signal for the publisher to stop publishing
	close(closeCh)

	logger.Info("All messages published", nil)
}

func publishWeatherData(publisher message.Publisher, closeCh chan struct{}) {
	//call the Weather API every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		//if a signal has been sent through closeCh, publisher will stop publishing
		case <-closeCh:
			ticker.Stop()
			return
		case <-ticker.C:
		}

		//call the Weather API to get the weather data
		weatherData, err := GetCurrentWeather()
		if err != nil {
			log.Println("Issue retrieving weather data: ", err)
			continue
		}

		//marshall the weather data
		payload, err := json.Marshal(weatherData)
		if err != nil {
			log.Println("Could not marshall weatherData: ", err)
			continue
		}

		//construct a watermill message
		msg := message.NewMessage(watermill.NewUUID(), payload)

		//Use a middleware to set the correlation ID, it's useful for debugging
		middleware.SetCorrelationID(watermill.NewShortUUID(), msg)

		//publish the message to the "current weather" topic
		err = publisher.Publish("current_weather", msg)
		if err != nil {
			log.Println("cannot publish message: ", err)
			continue
		}
	}
}

func GetCurrentWeather() (ApiWeatherInfo, error) {
	req, err := http.NewRequest("GET", "http://api.weatherapi.com/v1/current.json?", nil)
	if err != nil {
		log.Println(err)
		return ApiWeatherInfo{}, err
	}

	// define the query parameters and their respective values
	q := req.URL.Query()
	q.Add("key", os.Getenv("WAPIKEY"))
	q.Add("q", "Washington DC")
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ApiWeatherInfo{}, err
	}
	defer resp.Body.Close()

	log.Println("Response Status: ", resp.Status)
	if resp.StatusCode != 200 {
		return ApiWeatherInfo{}, errors.New("did not receive 200 response code")
	}
	body, _ := ioutil.ReadAll(resp.Body)
	// unmarshall the body into a Response struct
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		return ApiWeatherInfo{}, err
	}
	//extract the information from the Response struct and create a ApiWeatherInfo struct
	current := response.Current
	currentWeatherInfo := ApiWeatherInfo{
		LastUpdated:   current.LastUpdated,
		Temperature:   current.TempF,
		FeelsLike:     current.FeelslikeF,
		Humidity:      current.Humidity,
		Condition:     current.Condition.Text,
		WindMph:       current.WindMph,
		WindDirection: current.WindDir,
		Visibility:    current.VisMiles,
		Precipitation: current.PrecipIn,
	}
	return currentWeatherInfo, nil
}

type Response struct {
	Current Current `json:"current,omitempty"`
}

type Current struct {
	LastUpdated string            `json:"last_updated,omitempty"`
	TempF       float64           `json:"temp_f,omitempty"`
	Condition   *CurrentCondition `json:"condition,omitempty"`
	WindMph     float64           `json:"wind_mph,omitempty"`
	WindDir     string            `json:"wind_dir,omitempty"`
	PrecipIn    float64           `json:"precip_in,omitempty"`
	Humidity    int32             `json:"humidity,omitempty"`
	FeelslikeF  float64           `json:"feelslike_f,omitempty"`
	VisMiles    float64           `json:"vis_miles,omitempty"`
}

type CurrentCondition struct {
	Text string `json:"text,omitempty"`
}

type ApiWeatherInfo struct {
	LastUpdated   string
	Temperature   float64
	FeelsLike     float64
	Humidity      int32
	Condition     string
	WindMph       float64
	WindDirection string
	Visibility    float64
	Precipitation float64
}
