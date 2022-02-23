package main

import (
	"fmt"
	"net/http"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

func launchServer() error {
	err := http.ListenAndServe(":4015", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}
	return nil
}

func main() {
	log.Log().Msg("Running App")
	log.Log().Msg("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()

}
