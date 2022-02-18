package redis

import (
	"errors"
	"github.com/gomodule/redigo/redis"
	"log"
)

var VideoNoError = errors.New("no video found")

func VideoDisplay(id string) (*Video,error){

	conn := Pool.Get()
	defer conn.Close()
	values, err := redis.Values(conn.Do("HGETALL", "video:"+id))
	if err != nil {
		return nil, err
	} else if len(values) == 0 {
		return nil, VideoNoError
	}
	var video Video
	err = redis.ScanStruct(values,&video)
	if err != nil {
		return nil, err
	}
	return &video,nil
}

func AddLike(id string) error{
	conn := Pool.Get()
	defer conn.Close()
	exists, err := redis.Int(conn.Do("EXISTS", "video:"+id))
	if err != nil {
		return err
	} else if exists == 0 {
		return VideoNoError
	}
	err = conn.Send("MULTI")
	if err != nil {
		return err
	}
	err = conn.Send("HINCRBY", "video:"+id, "likes", 1)
	if err != nil {
		return err
	}
	err = conn.Send("ZINCRBY", "likes", 1, id)
	if err != nil {
		return err
	}
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func GetPopular(x int) ([]*Video , error){
	conn := Pool.Get()
	defer conn.Close()
	for {
		_, err := conn.Do("WATCH", "likes")
		if err != nil {
			return nil, err
		}
		ids, err := redis.Strings(conn.Do("ZREVRANGE", "likes", 0, x))
		if err != nil {
			return nil, err
		}
		err = conn.Send("MULTI")
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			err := conn.Send("HGETALL", "video:"+id)
			if err != nil {
				return nil, err
			}
		}
		replies, err := redis.Values(conn.Do("EXEC"))
		if err == redis.ErrNil {
			log.Println("trying again")
			continue
		} else if err != nil {
			return nil, err
		}
		videos := make([]*Video, x)
		for i, reply := range replies {
			var video Video
			if i == x{
				break
			}
			err = redis.ScanStruct(reply.([]interface{}), &video)
			if err != nil {
				return nil, err
			}
			videos[i] = &video
		}
		return videos, nil
	}
}

// Log time spent starting up, retrieving events, 
// retrieving redis values, and producing events

// Create Topics in Kafka (InboundTopic and OutboundTopic)
func CreateTopics(topics []string) error{
}

// Put InboundTopic

// Get OutboundTopic

// Put OutboundTopic

// Get InboundTopic

// Get Messages from Topic in Kafka
func GetMessages(topic string) error{
}

// Get Redis Value and Perform a Computation on it
func GetAndComputeRedisValue(value string) error{
	// Compute value
}
