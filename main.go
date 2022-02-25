package main

import (
	"fmt"
	"main/kafka"
	"main/redis"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

func launchServer() error {
	err := http.ListenAndServe(":4000", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}
	return nil
}

func main() {
	log.Log().Msg("App listening on port 4000")
	// log.Log().Msg("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()

}
