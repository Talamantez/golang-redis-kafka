# video-feed
Integrated Go Application with a simple server that serves sample video data for starters in kafka-redis-go.  


### To Run
* Follow  https://kafka.apache.org/documentation/ to setup a kafka cluster
* Follow  https://docs.confluent.io/current/clients/go.html for go client info
* Follow  https://redis.io/ to setup redis cluster on local
* In ```$GOTPATH/src/vendor-feed``` do  ```go mod vendor && go mod tidy``` for installation 

#### start server
* In ```$GOTPATH/src/vendor-feed``` start server using ``` go run .```



