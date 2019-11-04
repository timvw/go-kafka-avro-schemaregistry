package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/linkedin/goavro"
)

func getSchemaID(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
}

func getSubject(topic string) string {
	return fmt.Sprintf("%v-value", topic)
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

	schemaRegistryClient, err := schemaregistry.NewClient(schemaRegistryURL)

	if err != nil {
		panic(err)
	}

	c, err := kafka.NewConsumer(kafkaConfig)

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"test"}, nil)

	run := true

	for run == true {

		select {

		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		default:

			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {

			case *kafka.Message:

				magicByte := e.Value[0]

				if magicByte != 0 {
					fmt.Fprintf(os.Stderr, "Error: Unknown macic byte!")
				} else {

					schemaID := getSchemaID(e.Value[1:5])
					subject := getSubject(*e.TopicPartition.Topic)

					schema, err := schemaRegistryClient.GetSchemaBySubject(subject, schemaID)

					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: could not find schema for %v", err)
					}

					data := e.Value[5:]

					codec, err := goavro.NewCodec(schema.Schema)

					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: could not create codec %v", err)
					}
					native, _, err := codec.NativeFromBinary(data)

					if err != nil {
						fmt.Println(err)
					}

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
	c.Close()

}
