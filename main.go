package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
	schemaregistry "github.com/lensesio/schema-registry"
)

type SubjectVersionID struct {
	subject   string
	versionID int
}

func extractSubjectAndVersionFromData(topic string, isKey bool, data []byte) (key SubjectVersionID, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	var subject string
	if isKey {
		subject = fmt.Sprintf("%v-key", topic)
	} else {
		subject = fmt.Sprintf("%v-value", topic)
	}

	versionID := getSchemaID(data[1:5])

	key = SubjectVersionID{subject, versionID}
	return
}

func extractSubjectAndVersionFromValue(message *kafka.Message) (key SubjectVersionID, err error) {
	return extractSubjectAndVersionFromData(*message.TopicPartition.Topic, false, message.Value)
}

func extractSubjectAndVersionFromKey(message *kafka.Message) (key SubjectVersionID, err error) {
	return extractSubjectAndVersionFromData(*message.TopicPartition.Topic, false, message.Key)
}

func getSchemaID(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
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
	cachedSchemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryClient)
	avroCodec := NewAvroCodec(cachedSchemaRegistryClient)

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

				cacheKey, err := extractSubjectAndVersionFromValue(e)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: Could not extract subject and version from value %v, %v", e, err)
				}

				codec, err := avroCodec.GetCodecFor(cacheKey)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: could not create codec %v", err)
				}

				data := e.Value[5:]
				native, _, err := codec.NativeFromBinary(data)

				if err != nil {
					fmt.Println(err)
				}

				fmt.Printf("Message on %s: %s\n", e.TopicPartition, native)

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
