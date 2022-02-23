package main

import (
	"fmt"
	"net/http"
	"os"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/gorilla/mux"
	"github.com/gyozatech/noodlog"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var nlog *noodlog.Logger

func init() {
	nlog = noodlog.NewLogger().SetConfigs(
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
	err := http.ListenAndServe(":4015", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}

	return nil
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Running App")
	nlog.Trace("Running App")
	log.Info().Msg("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")
	nlog.Trace("Localhost Endpoint: {your port}/produce-to-incoming-topic?message={your message}")

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})

	launchServer()
}
