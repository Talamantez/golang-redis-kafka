package main

//////////
import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"video-feed/kafka"
	"video-feed/redis"

	"github.com/CrowdSurge/banner"
	"github.com/gorilla/mux"
)

func main() {
	if len(os.Args) > 2 {
		if os.Args[1] == "populate" {
			numRecords, err := strconv.Atoi(os.Args[2])
			if err != nil {
				fmt.Println("Could not convert arguments provided, hence creating four entries")
				numRecords = 4
			}
			redis.Populate(numRecords)
			return
		}
	}

	router := mux.NewRouter()
	router.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pong !!\n"))
	})

	router.HandleFunc("/produce-to-outgoing-topic", ProduceToOutgoingTopic)

	http.Handle("/", router)

	banner.Print("redis kafka")

	log.Println("Initializing redis pool: ")
	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"messages"})
	log.Println("Messages Topic App Listening on :4000")
	err := http.ListenAndServe(":4000", nil)
	if err != nil {
		log.Printf("Server error %v :", err)
	}
}
