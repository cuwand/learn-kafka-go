package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	goKafka "github.com/Shopify/sarama"
	"time"
)

var producerKafka goKafka.SyncProducer

type KafkaClient struct {
	Username string
	Password string
	Address  string
}

func NewKafkaClient(client KafkaClient) KafkaClient {
	return KafkaClient{
		Address:  client.Address,
		Password: client.Password,
		Username: client.Username,
	}
}

func (k KafkaClient) getKafkaConfig() *goKafka.Config {
	kafkaConfig := goKafka.NewConfig()

	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Net.WriteTimeout = 1 * time.Second

	if k.Username != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = k.Username
		kafkaConfig.Net.SASL.Password = k.Password
	}

	version, err := goKafka.ParseKafkaVersion("2.1.1")
	if err != nil {
		panic(fmt.Sprintf("Error parsing Kafka version: %v", err))
	}

	kafkaConfig.Version = version
	kafkaConfig.Consumer.Offsets.Initial = goKafka.OffsetOldest
	kafkaConfig.Consumer.Group.Rebalance.Strategy = goKafka.BalanceStrategyRange

	return kafkaConfig
}

func (k KafkaClient) isAvailableTopic(topic string) (bool, error) {
	clusterAdmin, err := goKafka.NewClusterAdmin([]string{k.Address}, k.getKafkaConfig())

	if err != nil {
		return false, err
	}

	topicList, err := clusterAdmin.ListTopics()

	if err != nil {
		return false, err
	}

	_, ok := topicList[topic]

	if ok {
		return true, nil
	}

	return false, nil
}

func (k KafkaClient) createTopic(topic string, partition int32) error {
	clusterAdmin, err := goKafka.NewClusterAdmin([]string{k.Address}, k.getKafkaConfig())

	if err != nil {
		return err
	}

	exist, err := k.isAvailableTopic(topic)

	if err != nil {
		return err
	}

	if exist {
		return nil
	}

	err = clusterAdmin.CreateTopic(topic, &goKafka.TopicDetail{
		NumPartitions:     partition,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		return err
	}

	return nil
}

func (k KafkaClient) Publish(topic string, partition int32, payload interface{}) (*int32, error) {
	if producerKafka == nil {
		producer, err := goKafka.NewSyncProducer([]string{k.Address}, k.getKafkaConfig())

		if err != nil {
			return nil, err
		}

		producerKafka = producer
	}

	err := k.createTopic(topic, partition)

	if err != nil {
		return nil, err
	}

	byteData, err := json.Marshal(payload)

	if err != nil {
		return nil, err
	}

	partititon, _, err := producerKafka.SendMessage(&goKafka.ProducerMessage{
		Topic: topic,
		Value: goKafka.ByteEncoder(byteData),
	})

	if err != nil {
		return nil, err
	}

	return &partititon, err
}

func (k KafkaClient) RegisterConsumer(groupId, topic string, handler HandlerFunc) {
	for {
		isAvailableTopic, err := k.isAvailableTopic(topic)

		if err != nil {
			panic(err)
		}

		if !isAvailableTopic {
			fmt.Println("topic not found, consumer sleep 15s")
			time.Sleep(15 * time.Second)
			continue
		}

		break
	}

	cluster, err := goKafka.NewConsumerGroup([]string{k.Address}, groupId, k.getKafkaConfig())

	if err != nil {
		panic(err)
	}

	defer cluster.Close()
	fmt.Println("Consumer is Running....")

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := ConsumerGroup{
		handler: handler,
	}

	for {
		err := cluster.Consume(context.Background(), []string{topic}, &consumer)

		if err != nil {
			panic(err)
		}
	}
}

// -- config consumer group

type Message struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

type HandlerConsumer interface {
	HandleMessage(message *Message) error
}

/**
 * HandlerFunc is a convenience type to avoid having to declare a struct
 * to implement the Handler interface, it can be used like this:
 *
 * handler := qKafka.HandlerFunc(func(m *qKafka.Message) error {
 * 		   var age int
 *  	   if err := json.Unmarshal(m.Value, &age); err != nil {
 * 				  return err
 *		   }
 *		   return nil
 * })
 **/
type HandlerFunc func(message *Message) error

/**
 * HandleMessage implements the Handler interface
 **/
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// Consumer represents a Sarama consumer group consumer
type ConsumerGroup struct {
	handler HandlerFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerGroup) Setup(goKafka.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerGroup) Cleanup(goKafka.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerGroup) ConsumeClaim(session goKafka.ConsumerGroupSession, claim goKafka.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine
	for message := range claim.Messages() {
		err := consumer.handler.HandleMessage(&Message{
			Value:     message.Value,
			Topic:     message.Topic,
			Key:       message.Key,
			Offset:    message.Offset,
			Partition: message.Partition,
		})

		if err != nil {
			return err
		}

		session.MarkMessage(message, "")
	}

	return nil
}
