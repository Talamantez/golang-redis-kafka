package kafka

import (
	"fmt"
	"net/http"
)

func ProduceToTelemetryTopic(w http.ResponseWriter, r *http.Request) {
	message, ok := r.URL.Query()["message"]
	if !ok || len(message[0]) < 1 {
		fmt.Println("Url Param 'message' is missing")
		return
	}
	go Produce("InboundTopic", message[0])
}
