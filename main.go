package main

import (
	"fmt"
	"io"
	"main/kafka"
	"main/redis"
	"net/http"
	"os"
	"time"

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

type Report struct {
	SetTopicInRedis      int32
	ReadTopicFromRedis   int32
	ProducedTopicToKafka int32
	LoggedAt             time.Time
}

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

	// Over-write log file if it exists, otherwise create the file
	file, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	// file, err := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
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
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic", "TelemetryTopic"})

	launchServer()

}
