package redis

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

func SetRedisTopic(topic string, message string) error {
	start := time.Now()

	conn := Pool.Get()
	defer conn.Close()
	key := topic
	value := message
	_, err := redis.String(conn.Do("SET", key, value, "EX", 6000))
	if err != nil {
		return err
	}
	end := time.Now()
	duration := end.Sub(start)
	isInMicroseconds := strings.Contains(duration.String(), "\u00B5")

	myDuration := duration.String()[:len(duration.String())-2]
	log.SetFormatter(&log.JSONFormatter{})
	// If the file doesn't exist, create it or append to the file
	file, err := os.OpenFile("logs.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)
	var n int32
	fmt.Sscan(myDuration, &n)
	fmt.Println("\n")
	fmt.Println(n == 100)
	if isInMicroseconds {
		log.WithFields(
			log.Fields{
				"SetTopicInRedis": n / 1000,
			},
		).Println("Converted microseconds to milliseconds")
	} else {
		log.WithFields(
			log.Fields{
				"SetTopicInRedis": n,
			},
		).Println("")
	}
	return nil
}

func GetRedisTopic(topic string) (string, error) {
	start := time.Now()

	conn := Pool.Get()
	defer conn.Close()
	message, err := redis.String(conn.Do("GET", topic))
	if err != nil {
		return "", err
	}
	end := time.Now()
	duration := end.Sub(start)
	isInMicroseconds := strings.Contains(duration.String(), "\u00B5")

	myDuration := duration.String()[:len(duration.String())-2]
	log.SetFormatter(&log.JSONFormatter{})
	// If the file doesn't exist, create it or append to the file
	file, err := os.OpenFile("logs.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)
	var n int32
	fmt.Sscan(myDuration, &n)
	fmt.Println("\n********* ")
	fmt.Println(n == 100)
	if isInMicroseconds {
		log.WithFields(
			log.Fields{
				"ReadTopicFromRedis": n / 1000,
			},
		).Println("Converted microseconds to milliseconds")
	} else {
		log.WithFields(
			log.Fields{
				"ReadTopicFromRedis": n,
			},
		).Println("")
	}

	return message, nil
}
