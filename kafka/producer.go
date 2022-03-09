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
		Headers:        []kafka.Header{{Key: "Producer", Value: []byte("Producer")}},
	}, deliveryChan)

	e := <-deliveryChan

	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		end := time.Now()
		duration := end.Sub(start)

		// We want to convert microseconds to milliseconds,
		// so first check if the duration is in milliseconds
		// by checking for the 'mu' special character '\u00B5'
		isInMicroseconds := strings.Contains(duration.String(), "\u00B5")

		// remove the unit from the number
		myDuration := duration.String()[:len(duration.String())-2]

		log.SetFormatter(&log.JSONFormatter{})
		// If the file doesn't exist, create it or append to the file
		file, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		mw := io.MultiWriter(os.Stdout, file)
		log.SetOutput(mw)
		var n int32
		fmt.Sscan(myDuration, &n)
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
