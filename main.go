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

type schemaCacheKey struct {
	subject   string
	versionID int
}

func extractSubjectAndVersionFromData(topic string, isKey bool, data []byte) (subject string, versionID int, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	if isKey {
		subject = fmt.Sprintf("%v-key", topic)
	} else {
		subject = fmt.Sprintf("%v-value", topic)
	}

	versionID = getSchemaID(data[1:5])

	return
}

func extractSubjectAndVersionFromValue(message *kafka.Message) (subject string, versionID int, err error) {
	return extractSubjectAndVersionFromData(*message.TopicPartition.Topic, false, message.Value)
}

func extractSubjectAndVersionFromKey(message *kafka.Message) (subject string, versionID int, err error) {
	return extractSubjectAndVersionFromData(*message.TopicPartition.Topic, false, message.Key)
}

func getSchemaBySubject(subject string, versionID int, schemaCache map[schemaCacheKey]string, client *schemaregistry.Client) (schema string, err error) {

	key := schemaCacheKey{subject, versionID}

	schema, ok := schemaCache[key]

	if !ok {
		var avroSchema schemaregistry.Schema
		avroSchema, err = client.GetSchemaBySubject(subject, versionID)
		if err != nil {
			return
		}
		schema = avroSchema.Schema
		schemaCache[key] = schema
	}

	return
}

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
	schemaCache := make(map[schemaCacheKey]string)

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

				subject, versionID, err := extractSubjectAndVersionFromValue(e)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: Could not extract subject and version from value %v, %v", e, err)
				}

				schema, err := getSchemaBySubject(subject, versionID, schemaCache, schemaRegistryClient)


				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: could not find schema for %v", err)
				}

				data := e.Value[5:]

				codec, err := goavro.NewCodec(schema)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: could not create codec %v", err)
				}
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
