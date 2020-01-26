package main

import (
	"encoding/json"
	"fmt"
	"github.com/cuwand/learn-kafka-go/pkg/kafka"
)

func main() {
	kafkaClient := kafka.NewKafkaClient(kafka.KafkaClient{
		Username: "",
		Password: "",
		Address:  "127.0.0.1:9092",
	})

	fmt.Println("Starting Consumer......")

	go kafkaClient.RegisterConsumer("minggu2-group", "minggu2", kafka.HandlerFunc(func(m *kafka.Message) error {
		var msg string

		if err := json.Unmarshal(m.Value, &msg); err != nil {
			return err
		}

		fmt.Println("From Consumer 1 ->> ", msg)

		return nil
	}))

	go kafkaClient.RegisterConsumer("minggu2-group", "minggu2", kafka.HandlerFunc(func(m *kafka.Message) error {
		var msg string

		if err := json.Unmarshal(m.Value, &msg); err != nil {
			return err
		}

		fmt.Println("From Consumer 2 ->> ", msg)

		return nil
	}))

	go kafkaClient.RegisterConsumer("minggu2-group", "minggu2", kafka.HandlerFunc(func(m *kafka.Message) error {
		var msg string

		if err := json.Unmarshal(m.Value, &msg); err != nil {
			return err
		}

		fmt.Println("From Consumer 3 ->> ", msg)

		return nil
	}))

	fmt.Scanln()
}
