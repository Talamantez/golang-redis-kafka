package kafka

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

var producer *kafka.Producer

func InitProducer() {

	broker := os.Getenv("PRODUCER")

	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		log.Print("Failed to create producer")
		panic(err)
	}
}

func Produce(topic string, value string) {
	start := time.Now()

	deliveryChan := make(chan kafka.Event)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "InboundTopic", Value: []byte("InboundTopic feed header value")}},
	}, deliveryChan)

	e := <-deliveryChan

	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		end := time.Now()
		duration := end.Sub(start)
		// If duration is in microseconds, convert to milliseconds
		isInMicroseconds := strings.Contains(duration.String(), "\u00B5")

		// strip the unit
		myDuration := duration.String()[:len(duration.String())-2]

		log.SetFormatter(&log.JSONFormatter{})
		// If the file doesn't exist, create it or append to the file
		file, err := os.OpenFile("logs.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		mw := io.MultiWriter(os.Stdout, file)
		log.SetOutput(mw)
		var n int32
		fmt.Sscan(myDuration, &n)
		fmt.Println("\n")
		fmt.Println(n == 100)
		if isInMicroseconds {
			log.WithFields(
				log.Fields{
					"ProducedTopicToKafka": n / 1000,
				},
			).Println("Converted microseconds to milliseconds")
		} else {
			log.WithFields(
				log.Fields{
					"ProducedTopicToKafka": n,
				},
			).Println("")
		}

	}
	if err != nil {
		log.Printf("Error in writing value : %v ", err)
	}
	close(deliveryChan)
}
