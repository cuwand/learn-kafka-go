package main

import (
	"bufio"
	"fmt"
	"github.com/cuwand/learn-kafka-go/pkg/kafka"
	"os"
)

func main() {
	kafkaClient := kafka.NewKafkaClient(kafka.KafkaClient{
		Username: "",
		Password: "",
		Address:  "127.0.0.1:9092",
	})

	var topic string
	var partition int32

	fmt.Println("Create Topic Name : ")
	fmt.Scan(&topic)
	fmt.Println("Partition : ")
	fmt.Scan(&partition)
	fmt.Println(fmt.Sprintf("\n-------- Topic Name: %v --------", topic))

	var counter int = 1
	for {
		var body string

		scanner := bufio.NewScanner(os.Stdin)

		fmt.Println(fmt.Sprintf("Create Message %v :", counter))
		for scanner.Scan() {
			body = scanner.Text()
			break
		}

		partition, err := kafkaClient.Publish(topic, partition, body)

		if err != nil {
			panic(err.Error())
		}

		fmt.Println(fmt.Sprintf("----- Message Created, at partition %v ", *partition))

		counter++
	}
}
