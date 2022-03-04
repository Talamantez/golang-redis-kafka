package redis

import (
	"time"
	"os"

	"github.com/gomodule/redigo/redis"
)

var Pool *redis.Pool

func Init() {
	Pool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", os.Getenv("REDIS_ADDRESS"))
		},
	}
}
