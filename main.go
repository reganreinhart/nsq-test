package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nsqio/go-nsq"
)

var producer *nsq.Producer
var consumer *nsq.Consumer

type messageHandler struct{}

func (mh *messageHandler) HandleMessage(m *nsq.Message) error {
	fmt.Println(string(m.Body))
	return nil
}

func test(w http.ResponseWriter, r *http.Request) {
	messageBody := []byte("hello")
	topicName := "topic"

	err := producer.Publish(topicName, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	delay := 10 * time.Second
	err = producer.DeferredPublish(topicName, delay, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	json.NewEncoder(w).Encode("test")
}

func main() {
	var err error
	conf := nsq.NewConfig()

	producer, err = nsq.NewProducer("localhost:32771", conf)
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Stop()

	consumer, err = nsq.NewConsumer("topic", "channel", conf)
	consumer.AddHandler(&messageHandler{})

	err = consumer.ConnectToNSQD("localhost:32771")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/test", test)
	http.ListenAndServe(":20000", nil)
}
