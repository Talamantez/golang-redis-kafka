package main

import (
	"fmt"
	"net/http"
	"os"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// var log *noodlog.Logger

// func init() {
// 	log = noodlog.NewLogger().SetConfigs(
// 		noodlog.Configs{
// 			LogLevel:             noodlog.LevelTrace,
// 			JSONPrettyPrint:      noodlog.Enable,
// 			TraceCaller:          noodlog.Enable,
// 			Colors:               noodlog.Enable,
// 			CustomColors:         &noodlog.CustomColors{Trace: noodlog.Cyan},
// 			ObscureSensitiveData: noodlog.Enable,
// 			SensitiveParams:      []string{"password"},
// 		},
// 	)
// }

func launchServer() error {
	// noodlog
	// log.Trace("Launching server")

	// overlog
	err := http.ListenAndServe(":4015", nil)
	if err != nil {
		fmt.Printf("Server error %v :", err)
	}

	return nil
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Str("foo", "bar").Msg("Hello world")

	router := mux.NewRouter()

	router.HandleFunc("/produce-to-incoming-topic", ProduceToIncomingTopic)

	http.Handle("/", router)

	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"InboundTopic", "OutboundTopic"})
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	launchServer()
}
