package kafka

import "C"
import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"video-feed/redis"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gyozatech/noodlog"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

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

func Consumer(topics []string) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	group := "InboundTopic"
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	broker := "localhost:9092,localhost:9093"
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
	}
	// log.Printf("Created Consumer %v", c)
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
					saveRedisTriggerOutboundTopicKafka(*e.TopicPartition.Topic, string(e.Value))
				} else {
					emitToRedis(*e.TopicPartition.Topic, string(e.Value))
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
	start := time.Now()
	err := redis.SetRedisTopic(topic, value)
	if err != nil {
		log.Print(err)
	} else {
		end := time.Now()
		duration := end.Sub(start)
		log := zerolog.New(os.Stdout).With().
			Timestamp().
			Str("app", "KafRedigo").Dur("Duration", duration).
			Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

		log.Info().Str(topic, value).Msg("Successfully set topic in redis")
		nlog.Trace("Successfully set topic in redis")

	}
}
func readFromRedis(topic string) (string, error) {
	startTime := time.Now()
	result, err := redis.GetRedisTopic(topic)
	if err != nil {
		return "", err
	}
	endTime := time.Now()

	diff := endTime.Sub(startTime)

	log := zerolog.New(os.Stdout).With().Dur("Duration", diff).
		Timestamp().
		Str("app", "KafRedigo").
		Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Print("Successfully read from Redis")
	nlog.Trace("Successfully read from Redis")

	return result, nil
}
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
	startTime := time.Now()
	Produce("OutboundTopic", str)
	endTime := time.Now()

	diff := endTime.Sub(startTime)

	log := zerolog.New(os.Stdout).With().Dur("Duration", diff).
		Timestamp().
		Str("app", "KafRedigo").
		Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Print("Produced to OutboundTopic Topic")
	nlog.Trace("Produced to OutboundTopic Topic")
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
