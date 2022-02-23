package kafka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

var producer *kafka.Producer

func InitProducer() {

	broker := "localhost:9092"

	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		log.Print("Failed to create producer")
		panic(err)
	}
}

func Produce(topic string, value string) {
	start := time.Now()
	myStart := fmt.Sprintf("%v", start)

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
		myEnd := fmt.Sprintf("%v", end)
		duration := end.Sub(start)
		myDuration := fmt.Sprintf("%v", duration)
		log.Log().Str("duration", myDuration).Str("start-time", myStart).Str("end-time", myEnd).Msg("*** PRODUCED-TOPIC-TO-KAFKA ***")
	}
	if err != nil {
		log.Printf("Error in writing value : %v ", err)
	}
	close(deliveryChan)
}
