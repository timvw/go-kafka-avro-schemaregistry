# kafkaavro

kafkaavro is a go library that decodes Kafka Avro data.


## Description

* Leverages [schema-registry](https://github.com/lensesio/schema-registry) client and [goavro](https://github.com/linkedin/goavro) 
to decode binary Avro data into native go data.
 
## Usage

The Codec is typically used to decode (value/key) kafka avro data coming from [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

* Import the required modules:

```go
import (
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/timvw/gokafkaavro"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)
```

* Define a type alias and add some utility functions: 

```go
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
```

* Instantiate a ConfluentAvroCodec:

```go
schemaRegistryURL := "http://localhost:8081"
cachedSchemaRegistryClient := gokafkaavro.NewCachedSchemaRegistryClient(client)
avroCodec := gokafkaavro.NewCodec(cachedSchemaRegistryClient)
confluentAvroCodec := (*ConfluentAvroCodec)(avroCodec)
```
	
* Use the codec to decode the value/key of a message:

```go
var msg kafka.Message
native, err := DecodeValue(avroCodec, msg)
```

Full code can be found in [./examples/main.go](./examples/main.go).
 
 ## Resources
* [Kafka avro wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
* [schema-registry](https://github.com/lensesio/schema-registry) The schema registry client implementation (see their docs to enable TLS etc)
* [goavro](https://github.com/linkedin/goavro) The module which encodes/decodes between go and avro
* [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) The module to consume from a kafka broker
* [librdkafka](https://github.com/edenhill/librdkafka) The Apache Kafka C/C++ library




