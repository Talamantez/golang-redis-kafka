# golang-redis-kafka
Integrated Go Application that allows the user to manage Kafka Producers, Consumers, and Topics. Topic data is cached in Redis. Fork of praguna/video-feed. 


### To Run
* Follow  https://kafka.apache.org/documentation/ to setup a kafka cluster
* Follow  https://docs.confluent.io/current/clients/go.html for go client info
* Follow  https://redis.io/ to setup redis cluster on local
* In ```$GOTPATH/src/vendor-feed``` do  ```go mod vendor && go mod tidy``` for installation 

#### start server
* In ```$GOTPATH/src/vendor-feed``` start server using ``` go run .```



