# kafkaavro

kafkaavro is a go library that glues [schema-registry](https://github.com/lensesio/schema-registry) client and [goavro](https://github.com/linkedin/goavro) 
in order to encode/decode Kafka Avro data.


## Description

* This library is not production-grade, battle-tested, but nothing more than a learning experiment for me :-)
 
## Usage

* Examples can be found here: [decode](./examples/decode/main.go) and [encode](./examples/encode/main.go)
 
 ## Resources
* [Kafka avro wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
* [schema-registry](https://github.com/lensesio/schema-registry) The schema registry client implementation (see their docs to enable TLS etc)
* [goavro](https://github.com/linkedin/goavro) The module which encodes/decodes between go and avro
* [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) The module to consume from a kafka broker
* [librdkafka](https://github.com/edenhill/librdkafka) The Apache Kafka C/C++ library




