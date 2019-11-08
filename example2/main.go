package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/linkedin/goavro"
	"github.com/timvw/gokafkaavro"
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

	cachedSchemaRegistryClient := gokafkaavro.NewCachedSchemaRegistryClient(client)
	avroCodec := gokafkaavro.NewCodec(cachedSchemaRegistryClient)
	//confluentAvroCodec := (*ConfluentAvroCodec)(avroCodec)

	nativeData := map[string]interface{}{
		"f1": "blahblah",
	}

	value, err := avroCodec.Encode(topic, false, schema, nativeData)

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

func main2() {

	/*
	schemaRegistryURL := "http://localhost:8081"

	client, err := schemaregistry.NewClient(schemaRegistryURL)
	if err != nil {
		return
	}

	cachedSchemaRegistryClient := gokafkaavro.NewCachedSchemaRegistryClient(client)
	avroCodec := gokafkaavro.NewCodec(cachedSchemaRegistryClient)
	confluentAvroCodec := (*ConfluentAvroCodec)(avroCodec)

	 */

	/*
	schema := `
        {
          "type": "record",
          "name": "LongList",
          "fields" : [
            {"name": "next", "type": ["null", "LongList"], "default": null}
          ]
        }`

	 */

	schema := `
		{
			"type":"record",
			"name":"myrecord",
			"fields":[
				{"name":"f1","type":"string"}
			]
		}`

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return
	}

	avro, err := codec.TextualFromNative(nil, map[string]interface{}{
		"f1": "blahblah",
	})

	if err != nil {
		return
	}

	fmt.Println(string(avro))




}
