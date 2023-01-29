package main

import (
	"bytes"
	stdSQL "database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

type WeatherInfo struct {
	LastUpdated   string
	Temperature   float64
	FeelsLike     float64
	Humidity      int32
	Condition     string
	WindMph       float64
	WindDirection string
	Visibility    float64
	Precipitation float64
	CreatedAt     string
}

type postgresSchemaAdapter struct{}

func (p postgresSchemaAdapter) SchemaInitializingQueries(topic string) []string {
	//we already created the table so there is no need to implement this method
	return []string{}
}

func (p postgresSchemaAdapter) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (last_updated, temperature, feels_like, humidity, condition, wind_mph, wind_direction, visibility, precipitation, created_at) VALUES %s`,
		topic,
		strings.TrimRight(strings.Repeat(`($1,$2,$3,$4,$5,$6,$7,$8,$9,$10),`, len(msgs)), ","),
	)

	var args []interface{}
	for _, msg := range msgs {
		weatherInfo := WeatherInfo{}

		decoder := gob.NewDecoder(bytes.NewBuffer(msg.Payload))
		err := decoder.Decode(&weatherInfo)
		if err != nil {
			return "", nil, err
		}

		args = append(
			args,
			weatherInfo.LastUpdated,
			weatherInfo.Temperature,
			weatherInfo.FeelsLike,
			weatherInfo.Humidity,
			weatherInfo.Condition,
			weatherInfo.WindMph,
			weatherInfo.WindDirection,
			weatherInfo.Visibility,
			weatherInfo.Precipitation,
			weatherInfo.CreatedAt,
		)
	}

	return insertQuery, args, nil
}

func (p postgresSchemaAdapter) SelectQuery(topic string, consumerGroup string, offsetsAdapter sql.OffsetsAdapter) (string, []interface{}) {
	// No need to implement this method, as PostgreSQL subscriber is not used in this example.
	return "", nil
}

func (p postgresSchemaAdapter) UnmarshalMessage(row *stdSQL.Row) (offset int, msg *message.Message, err error) {
	return 0, nil, errors.New("not implemented")
}
