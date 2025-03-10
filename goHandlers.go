package main

import (
	"fmt"
	"main/kafka"
	"net/http"
)

func ProduceToIncomingTopic(w http.ResponseWriter, r *http.Request) {
	message, ok := r.URL.Query()["message"]
	if !ok || len(message[0]) < 1 {
		fmt.Println("Url Param 'message' is missing")
		return
	}
	go kafka.Produce("InboundTopic", message[0])
}
