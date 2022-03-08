package main

import (
	"fmt"
	"io"
	"main/kafka"
	"main/redis"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// Log format:
// [11:58 AM] Hung Duong
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
		os.Exit(1)
	}
	log.SetFormatter(&log.JSONFormatter{})
	// If the file doesn't exist, create it or append to the file
	file, err := os.OpenFile("logs.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()

}
