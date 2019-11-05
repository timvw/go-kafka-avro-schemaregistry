package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/timvw/gokafkaavro"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ConfluentAvroCodec is a wrapper around gokafkaavro.Codec with utility functions to Decode key/value from a kafka.Message
type ConfluentAvroCodec gokafkaavro.Codec

// DecodeValue returns a native datum value for the binary encoded byte slice in the message.Value field
// in accordance with the Avro schema attached to the data
// [wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format).
// On success, it returns the decoded datum and a nil error value.
// On error, it returns nil for the datum value and the error message.
func (c *ConfluentAvroCodec) DecodeValue(msg *kafka.Message) (native interface{}, err error) {
	return (*gokafkaavro.Codec)(c).Decode(*msg.TopicPartition.Topic, false, msg.Value)
}

// DecodeKey returns a native datum value for the binary encoded byte slice in the message.Key field
// in accordance with the Avro schema attached to the data
// [wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format).
// On success, it returns the decoded datum and a nil error value.
// On error, it returns nil for the datum value and the error message.
func (c *ConfluentAvroCodec) DecodeKey(msg *kafka.Message) (native interface{}, err error) {
	return (*gokafkaavro.Codec)(c).Decode(*msg.TopicPartition.Topic, true, msg.Key)
}

func main() {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	kafkaConfig := &kafka.ConfigMap{
		"metadata.broker.list": "localhost:9092",
		"group.id":             "go-test2",
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
	}

	schemaRegistryURL := "http://localhost:8081"

	client, err := schemaregistry.NewClient(schemaRegistryURL)
	if err != nil {
		return
	}

	cachedSchemaRegistryClient := gokafkaavro.NewCachedSchemaRegistryClient(client)
	avroCodec := gokafkaavro.NewCodec(cachedSchemaRegistryClient)
	confluentAvroCodec := (*ConfluentAvroCodec)(avroCodec)

	kafkaConsumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		panic(err)
	}

	kafkaConsumer.SubscribeTopics([]string{"test"}, nil)

	run := true

	for run == true {

		select {

		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		default:

			ev := kafkaConsumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {

			case *kafka.Message:

				native, err := confluentAvroCodec.DecodeValue(e)

				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("Message on %s: %s\n", e.TopicPartition, native)
				}

			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}

			default:
				fmt.Printf("Ignored %v\n", e)
			}

		}
	}

	fmt.Printf("Closing consumer\n")
	kafkaConsumer.Close()

}
