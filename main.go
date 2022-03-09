package main

import (
	"fmt"
	"main/kafka"
	"main/redis"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// Log format:
// {
// "SetTopicInRedis": "20",
// "ReadTopicFromRedis": "60",
// "ProducedTopicToKafka": "100"
// }

func launchServer() error {
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}
	return nil
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("Error reading env file: %s", err)
		os.Exit(1)
	}
	log.SetFormatter(&log.JSONFormatter{})

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic", "TelemetryTopic"})

	launchServer()
}
