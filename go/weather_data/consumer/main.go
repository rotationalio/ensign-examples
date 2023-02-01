package main

import (
	"bytes"
	"context"
	stdSQL "database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	logger = watermill.NewStdLogger(false, false)
	//topic the subscriber is listening to
	weather_api_topic = "current_weather"
	//weather_info is the topic to which the subscriber posts messages to be inserted into the database
	weather_insert_topic = "weather_info"
)

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

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	//SignalsHandler will gracefully shutdown Router when SIGTERM is received
	router.AddPlugin(plugin.SignalsHandler)
	//The Recoverer middleware handles panics from handlers
	router.AddMiddleware(middleware.Recoverer)

	postgresDB := createPostgresConnection()
	log.Println("added postgres connection and created weather_info table")
	subscriber := createSubscriber(logger)
	publisher := createPublisher(postgresDB)

	router.AddHandler(
		"weather_info_inserter",
		weather_api_topic, //subscriber topic
		subscriber,
		weather_insert_topic, //publisher topic
		publisher,
		dbHandler{db: postgresDB}.checkRecordExists, //handler function
	)

	if err = router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func createPostgresConnection() *stdSQL.DB {
	host := "weather_db"
	port := 5432
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := stdSQL.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	createQuery := `CREATE TABLE IF NOT EXISTS weather_info (
		id SERIAL NOT NULL PRIMARY KEY,
		last_updated VARCHAR(50) NOT NULL,
		temperature DECIMAL,
		feels_like DECIMAL,
		humidity INTEGER,
		condition VARCHAR(36),
		wind_mph DECIMAL,
		wind_direction VARCHAR(36),
		visibility DECIMAL,
		precipitation DECIMAL,
		created_at VARCHAR(100) NOT NULL
	);`
	_, err = db.ExecContext(context.Background(), createQuery)
	if err != nil {
		panic(err)
	}

	return db
}

func createSubscriber(logger watermill.LoggerAdapter) message.Subscriber {
	subscriber, err := ensign.NewSubscriber(
		ensign.SubscriberConfig{
			EnsignConfig: &ensign.Options{
				ClientID:     os.Getenv("ENSIGN_CLIENT_ID"),
				ClientSecret: os.Getenv("ENSIGN_CLIENT_SECRET"),
			},
			Unmarshaler: ensign.EventMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return subscriber
}

func createPublisher(db *stdSQL.DB) message.Publisher {
	pub, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter:        postgresSchemaAdapter{},
			AutoInitializeSchema: false, //false because the table has already been created
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return pub
}

type dbHandler struct {
	db *stdSQL.DB
}

func (d dbHandler) checkRecordExists(msg *message.Message) ([]*message.Message, error) {
	weatherInfo := ApiWeatherInfo{}

	err := json.Unmarshal(msg.Payload, &weatherInfo)
	if err != nil {
		return nil, err
	}

	log.Printf("received weather info: %+v", weatherInfo)

	var count int
	query := "SELECT count(*) FROM weather_info WHERE last_updated = $1"

	err = d.db.QueryRow(query, weatherInfo.LastUpdated).Scan(&count)
	switch {
	case err != nil:
		return nil, err
	default:
		if count > 0 {
			log.Println("Found existing record in the database")
			// not throwing an error here because this is not an issue
			return nil, nil
		}
		newWeatherInfo := WeatherInfo{
			LastUpdated:   weatherInfo.LastUpdated,
			Temperature:   weatherInfo.Temperature,
			FeelsLike:     weatherInfo.FeelsLike,
			Humidity:      weatherInfo.Humidity,
			Condition:     weatherInfo.Condition,
			WindMph:       weatherInfo.WindMph,
			WindDirection: weatherInfo.WindDirection,
			Visibility:    weatherInfo.Visibility,
			Precipitation: weatherInfo.Precipitation,
			CreatedAt:     time.Now().String(),
		}
		log.Println(newWeatherInfo)
		//encode the weather data
		var payload bytes.Buffer
		encoder := gob.NewEncoder(&payload)
		err := encoder.Encode(newWeatherInfo)
		if err != nil {
			panic(err)
		}
		//construct a watermill message
		newMessage := message.NewMessage(watermill.NewUUID(), payload.Bytes())
		return []*message.Message{newMessage}, nil
	}
}
