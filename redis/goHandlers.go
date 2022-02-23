package redis

import (
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
)

func SetRedisTopic(topic string, message string) error {
	startTime := time.Now()
	conn := Pool.Get()
	defer conn.Close()
	key := topic
	value := message
	_, err := redis.String(conn.Do("SET", key, value, "EX", 6000))
	if err != nil {
		return err
	}
	endTime := time.Now()

	diff := endTime.Sub(startTime)

	log := zerolog.New(os.Stdout).With().Dur("Duration", diff).
		Timestamp().
		Str("app", "KafRedigo").
		Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Print("Read from Redis")
	return nil
}

func GetRedisTopic(topic string) (string, error) {
	conn := Pool.Get()
	defer conn.Close()
	message, err := redis.String(conn.Do("GET", topic))
	if err != nil {
		return "", err
	}
	return message, nil
}
