# gokafkaavro

Gokafkaavro is a library that decodes Avro data.


## Description

* Leverages [schema-registry](https://github.com/lensesio/schema-registry) client and [goavro](https://github.com/linkedin/goavro) 
to decode binary Avro data.
 
## Usage

The Codec can be used to decode both key and value when using [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go):


```go

import (
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/timvw/gokafkaavro"
)

// DecodeValue decodes the value from the given kafka message
func DecodeValue(c *gokafkaavro.Codec, msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.Decode(*msg.TopicPartition.Topic, false, msg.Value)
}

// DecodeKey decodes the key from the given message
func DecodeKey(c *gokafkaavro.Codec, msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.Decode(*msg.TopicPartition.Topic, true, msg.Key)
}

schemaRegistryURL := "http://localhost:8081"
avroCodec, err := gokafkaavro.NewCodec(schemaRegistryURL)

var msg kafka.Message
native, _, err := DecodeValue(avroCodec, msg)
``` 

Full code can be found in [./examples/main.go](./examples/main.go).
 
 
 ## Resources
 * [Kafka avro wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)





