package redis

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

func SetRedisTopic(topic string, message string) error {
	start := time.Now()
	myStart := fmt.Sprintf("%v", start)

	conn := Pool.Get()
	defer conn.Close()
	key := topic
	value := message
	_, err := redis.String(conn.Do("SET", key, value, "EX", 6000))
	if err != nil {
		return err
	}
	end := time.Now()
	myEnd := fmt.Sprintf("%v", end)
	duration := end.Sub(start)
	myDuration := fmt.Sprintf("%v", duration)

	log.Log().Str("duration", myDuration).Str("start-time", myStart).Str("end-time", myEnd).Msg("*** SET-TOPIC-IN-REDIS ***")
	return nil
}

func GetRedisTopic(topic string) (string, error) {
	start := time.Now()
	myStart := fmt.Sprintf("%v", start)

	conn := Pool.Get()
	defer conn.Close()
	message, err := redis.String(conn.Do("GET", topic))
	if err != nil {
		return "", err
	}
	end := time.Now()
	myEnd := fmt.Sprintf("%v", end)
	duration := end.Sub(start)
	myDuration := fmt.Sprintf("%v", duration)

	log.Log().Str("duration", myDuration).Str("start-time", myStart).Str("end-time", myEnd).Msg("*** READ-TOPIC-FROM-REDIS ***")
	return message, nil
}
