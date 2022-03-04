# golang-redis-kafka
Integrated Go Application that allows the user to manage Kafka Producers, Consumers, and Topics. Topic data is cached in Redis. Fork of praguna/video-feed. 

### Run the Client Container
```docker run rtalamantez/kafredigo```

### Environment Variables
* Create an .env file with the following enviroment variables
```
# .env
PRODUCER="localhost:9092"
CONSUMER="localhost:9092,localhost:9093"
REDIS_ADDRESS="localhost:6379"```

### To Run
* Follow  https://kafka.apache.org/documentation/ to setup a kafka cluster
* Follow  https://docs.confluent.io/current/clients/go.html for go client info
* Follow  https://redis.io/ to setup redis cluster on local
* In ```$GOPATH/src/golang-redis-kafka``` do  ```go mod vendor && go mod tidy``` for installation 

#### start server
* In ```$GOPATH/src/golang-redis-kafka``` start server using ``` go run .```



