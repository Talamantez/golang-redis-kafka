package main

import (
	"fmt"
	"os"
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
	err := godotenv.Load(".env")
	if err != nil {
		log.Error().Msg("Error loading .env file - See README.md for Environment Variables")
		os.Exit(1)
	}
	log.Log().Msg("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")
	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()

	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()

}
