package main

import (
	"fmt"
	"net/http"
	"time"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
)

func launchServer() error {
	start := time.Now()

	err := http.ListenAndServe(":4002", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Println(duration)
	return nil
}

func main() {

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	launchServer()
}
