package main

import (
	"fmt"
	"net/http"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/gorilla/mux"
	"github.com/gyozatech/noodlog"
	"github.com/rs/zerolog"
)

var log *noodlog.Logger

func init() {
	log = noodlog.NewLogger().SetConfigs(
		noodlog.Configs{
			LogLevel:             noodlog.LevelTrace,
			JSONPrettyPrint:      noodlog.Enable,
			TraceCaller:          noodlog.Enable,
			Colors:               noodlog.Enable,
			CustomColors:         &noodlog.CustomColors{Trace: noodlog.Cyan},
			ObscureSensitiveData: noodlog.Enable,
			SensitiveParams:      []string{"password"},
		},
	)
}

func launchServer() error {

	err := http.ListenAndServe(":4014", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}

	return nil
}

func main() {
	// simple string message (with custom color)
	log.Trace("Hello world!")

	// chaining elements
	log.Info("You've reached", 3, "login attemps")

	// using string formatting
	log.Warn("You have %d attempts left", 2)

	// logging a struct with a JSON
	log.Error(struct {
		Code  int
		Error string
	}{500, "Generic Error"})

	// logging a raw JSON string with a JSON (with obscuring "password")
	log.Info(`{"username": "gyozatech", "password": "Gy0zApAssw0rd"}`)

	// logging a JSON string with a JSON (with obscuring "password")
	log.Info("{\"username\": \"nooduser\", \"password\": \"N0oDPasSw0rD\"}")
	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	launchServer()
}
