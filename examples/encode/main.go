package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/timvw/gokafkaavro"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "test"

	schema := `
		{
			"type":"record",
			"name":"myrecord",
			"fields":[
				{"name":"f1","type":"string"}
			]
		}`

	schemaRegistryURL := "http://localhost:8081"

	client, err := schemaregistry.NewClient(schemaRegistryURL)
	if err != nil {
		return
	}

	subjectNameStrategy := gokafkaavro.TopicNameStrategy{}
	subjectName := subjectNameStrategy.GetSubjectName(topic, false)

	encoder, err := gokafkaavro.NewEncoder(*client, true, subjectName, schema)
	if err != nil {
		fmt.Printf("failed to create encoder, %v", err)
	}

	nativeData := map[string]interface{}{
		"f1": "blahblah. this seems to work as expeccted",
	}

	value, err := encoder.Encode(nativeData)
	if err != nil {
		fmt.Printf("failed to encode, %v", err)
	}

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
