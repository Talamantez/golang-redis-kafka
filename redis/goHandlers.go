package redis

import (
	"errors"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
)

var VideoNoError = errors.New("no video found")

func VideoDisplay(id string) (*Video, error) {

	conn := Pool.Get()
	defer conn.Close()
	values, err := redis.Values(conn.Do("HGETALL", "video:"+id))
	if err != nil {
		return nil, err
	} else if len(values) == 0 {
		return nil, VideoNoError
	}
	var video Video
	err = redis.ScanStruct(values, &video)
	if err != nil {
		return nil, err
	}
	return &video, nil
}

func AddLike(id string) error {
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

func SetMessage(message string) error {
	fmt.Println("Setting messages in redis")
	conn := Pool.Get()
	defer conn.Close()
	key := "messages"
	value := message
	_, err := redis.String(conn.Do("SET", key, value, "EX", 6000))
	if err != nil {
		return err
	}
	return nil
}

func GetMessage(topic string) (string, error) {
	// Issue a HGET command to retrieve the message
	// and use the Str() helper method to convert the reply to a string.
	fmt.Println("getting message from ", topic)
	conn := Pool.Get()
	defer conn.Close()
	message, err := redis.String(conn.Do("GET", "messages"))
	if err != nil {
		return "", err
	} else {
		str := fmt.Sprintf("%v", message)
		fmt.Println(str)
	}
	fmt.Println(message)
	return message, nil
}

func GetPopular() ([]*Video, error) {
	conn := Pool.Get()
	x := 10
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
			if i == x {
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
