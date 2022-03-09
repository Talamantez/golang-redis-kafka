package kafka

import "C"
import (
	"bufio"
	"encoding/json"
	"fmt"
	"main/redis"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type Report struct {
	SetTopicInRedis      int32     `json:"SetTopicInRedis"`
	ReadTopicFromRedis   int32     `json:"ReadTopicFromRedis"`
	ProducedTopicToKafka int32     `json:"ProducedTopicToKafka"`
	LoggedAt             time.Time `json:"LoggedAt"`
}
type KafkaUpdate struct {
	ProducedTopicToKafka int32
}
type RedisSet struct {
	SetTopicInRedis int32
}
type RedisGet struct {
	ReadTopicFromRedis int32
}

func Consumer(topics []string) {
	group := "InboundTopic"
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	broker := os.Getenv("CONSUMER")
	// sslCaLocation := os.Getenv("SSL_CA_LOCATION")
	// securityProtocol := os.Getenv("SECURITY_PROTOCOL")
	// saslMechanism := os.Getenv("SASL_MECHANISM")
	// enableAutoCommit := os.Getenv("ENABLE_AUTO_COMMIT")
	// autoCommitIntervalMs := os.Getenv("AUTO_COMMIT_INTERVAL_MS")
	// autoOffsetReset := os.Getenv("AUTO_OFFSET_RESET")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// "ssl.ca.location":                 sslCaLocation,
		// "security.protocol":               securityProtocol,
		// "sasl.mechanism":                  saslMechanism,
		// "enable.auto.commit":              enableAutoCommit,
		// "auto.commit.interval.ms":         autoCommitIntervalMs,
		// "auto.offset.reset":               autoOffsetReset,
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
	}
	err = c.SubscribeTopics(topics, nil)

	for {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating", sig)
			_ = c.Close()
			os.Exit(1)

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				myTopic := *e.TopicPartition.Topic
				if myTopic == "InboundTopic" {
					// When the inbound topic updates,
					// we update Redis, perform business logic,
					// and produce those results to the outbound topic
					saveRedisTriggerOutboundTopicKafka(*e.TopicPartition.Topic, string(e.Value))
				} else if myTopic == "OutboundTopic" {
					// If the outbound topic has been
					// updated, then it's time to emit
					// the telemetry
					produceTelemetry()
				}
			case kafka.PartitionEOF:
				log.Printf("%% Reached %v", e)
			case kafka.Error:
				log.Printf("%% Error: %v", e)
			}
		}
	}
}
func emitToRedis(topic string, value string) {
	err := redis.SetRedisTopic(topic, value)
	if err != nil {
		log.Print(err)
	}
}
func readFromRedis(topic string) (string, error) {
	result, err := redis.GetRedisTopic(topic)
	if err != nil {
		return "", err
	}
	return result, nil
}

// TODO: Replace 'reverseString' with Damian's Calculation
func reverseString(str string) (string, error) {
	rune_arr := []rune(str)
	var rev []rune
	for i := len(rune_arr) - 1; i >= 0; i-- {
		rev = append(rev, rune_arr[i])
	}
	result := string(rev)
	return result, nil
}
func produceOutboundTopic(str string) {
	Produce("OutboundTopic", str)
}

func produceTelemetry() {
	// Read the log file
	file, err := os.Open("log.txt")
	if err != nil {
		log.Fatal()

	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}
	// Close the file
	file.Close()
	// Remove the file after this routine runs
	defer os.Remove("log.txt")

	p := Report{}
	var kafkaUpdate KafkaUpdate
	var redisSet RedisSet
	var redisGet RedisGet
	for _, each_ln := range text {
		if strings.Contains(each_ln, "ProducedTopicToKafka") {
			err := json.Unmarshal([]byte(each_ln), &kafkaUpdate)
			if err != nil {
				fmt.Println(err)
			}
			p.SetProducedToKafka(kafkaUpdate.ProducedTopicToKafka)
		} else if strings.Contains(each_ln, "SetTopicInRedis") {
			err := json.Unmarshal([]byte(each_ln), &redisSet)
			if err != nil {
				fmt.Println(err)
			}
			p.SavedToRedis(redisSet.SetTopicInRedis)
		} else if strings.Contains(each_ln, "ReadTopicFromRedis") {
			err := json.Unmarshal([]byte(each_ln), &redisSet)
			if err != nil {
				fmt.Println(err)
			}
			p.ReadFromRedis(redisGet.ReadTopicFromRedis)
		}
	}
	p.SetTime(time.Now())
	bytes, _ := json.Marshal(p)

	// Write the log to the topic
	Produce("TelemetryTopic", string(bytes))
}

func (f *Report) SetProducedToKafka(val int32) {
	f.ProducedTopicToKafka = val
}

func (f *Report) SavedToRedis(val int32) {
	f.SetTopicInRedis = val
}

func (f *Report) ReadFromRedis(val int32) {
	f.SetTopicInRedis = val
}

// Set receives a pointer to Report so it can modify it.
func (f *Report) SetTime(time time.Time) {
	f.LoggedAt = time
}

// SeeReadTopicFromRedis receives a copy of Report since it doesn't need to modify it.
func (f Report) SeeLoggedAt() time.Time {
	return f.LoggedAt
}

func saveRedisTriggerOutboundTopicKafka(topic string, value string) error {
	// save topic to redis
	emitToRedis(topic, value)

	// read topic from redis
	message, err := readFromRedis(topic)
	if err != nil {
		return err
	}

	// reverse message
	reversed, err := reverseString(message)
	if err != nil {
		return err
	}

	// produce reversed message to Kafka
	produceOutboundTopic(reversed)
	return nil
}
