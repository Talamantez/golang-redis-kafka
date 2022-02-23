package redis

import (
	"github.com/gomodule/redigo/redis"
)

func SetRedisTopic(topic string, message string) error {
	conn := Pool.Get()
	defer conn.Close()
	key := topic
	value := message
	_, err := redis.String(conn.Do("SET", key, value, "EX", 6000))
	if err != nil {
		return err
	}
	return nil
}

func GetRedisTopic(topic string) (string, error) {
	conn := Pool.Get()
	defer conn.Close()
	message, err := redis.String(conn.Do("GET", topic))
	if err != nil {
		return "BLOOB", err
	}
	return message, nil
}
