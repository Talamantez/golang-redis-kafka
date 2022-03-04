package main

import (
	"fmt"
	"main/kafka"
	"main/redis"
	"net/http"
	"github.com/joho/godotenv"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

func launchServer() error {
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}
	return nil
}

func main() {
	log.Log().Msg("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")
	err := godotenv.Load(".env")
	if err != nil {
		log.Log().Msg("Error loading .env file")
	  }
	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()

}
