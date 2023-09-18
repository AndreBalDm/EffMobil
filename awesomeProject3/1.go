package main

import (
	"context"
	"encoding/json"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"time"
)

type FIO struct {
	Name       string
	Surname    string
	Patrynomic string
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	// get kafka writer using environment variables.
	kafkaURL := "localhost:9092" //os.Getenv("kafkaURL")
	topic := "my-topic-1"        //os.Getenv("topic")
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")
	man := FIO{
		Name:       "Andrew",
		Surname:    "Bobov",
		Patrynomic: "Bebov",
	}
	manbytes, _ := json.Marshal(&man)
	var key = "Key-1"

	msg := kafka.Message{
		Key:   []byte(key),
		Value: manbytes,
	}
	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("produced", key)
	}
	time.Sleep(1 * time.Second)
}
