package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/linkedin/goavro"
)

// Codec decodes kafka avro messages using a schema registry
type Codec struct {
	schemaRegistryClient *cachedSchemaRegistryClient
	codecCache           map[subjectVersionID]*goavro.Codec
}

// NewCodec returns a new instance of Codec
func NewCodec(schemaRegistryURL string, options ...schemaregistry.Option) (codec *Codec, err error) {

	client, err := schemaregistry.NewClient(schemaRegistryURL, options...)
	if(err != nil) {
		return
	}

	cachedSchemaRegistryClient := newCachedSchemaRegistryClient(client)
	codec = &Codec{cachedSchemaRegistryClient, make(map[subjectVersionID]*goavro.Codec)}
	return
}

// DecodeValue decodes the value from the given kafka message
func (c *Codec) DecodeValue(msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.decode(*msg.TopicPartition.Topic, false, msg.Value)
}

// DecodeKey decodes the key from the given message
func (c *Codec) DecodeKey(msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.decode(*msg.TopicPartition.Topic, true, msg.Key)
}

type subjectVersionID struct {
	subject   string
	versionID int
}

func (c *Codec) decode(topic string, isKey bool, data []byte) (native interface{}, newBuf []byte, err error) {

	subjectVersion, err := extractSubjectAndVersionFromData(topic, isKey, data)
	if err != nil {
		return
	}

	codec, err := c.getCodecFor(subjectVersion)
	if err != nil {
		return
	}

	return codec.NativeFromBinary(data[5:])
}

func extractSubjectAndVersionFromData(topic string, isKey bool, data []byte) (key subjectVersionID, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	subject := getSubject(topic, isKey)
	versionID := getSchemaID(data[1:5])
	key = subjectVersionID{subject, versionID}
	return
}

func getSubject(topic string, isKey bool) (subject string) {
	if isKey {
		return fmt.Sprintf("%v-key", topic)
	}

	return fmt.Sprintf("%v-value", topic)
}

func getSchemaID(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
}

func (c *Codec) getCodecFor(subjectVersion subjectVersionID) (codec *goavro.Codec, err error) {

	codec, ok := c.codecCache[subjectVersion]

	if !ok {
		var schema string
		schema, err = c.schemaRegistryClient.getSchemaFor(subjectVersion)
		if err != nil {
			return
		}
		codec, err = goavro.NewCodec(schema)
		if err != nil {
			return
		}
		c.codecCache[subjectVersion] = codec
	}

	return
}

type cachedSchemaRegistryClient struct {
	client *schemaregistry.Client
	cache  map[subjectVersionID]string
}

func newCachedSchemaRegistryClient(client *schemaregistry.Client) (instance *cachedSchemaRegistryClient) {
	return &cachedSchemaRegistryClient{client, make(map[subjectVersionID]string)}
}

func (c *cachedSchemaRegistryClient) getSchemaFor(x subjectVersionID) (schema string, err error) {

	schema, ok := c.cache[x]

	if !ok {
		var avroSchema schemaregistry.Schema
		avroSchema, err = c.client.GetSchemaBySubject(x.subject, x.versionID)
		if err != nil {
			return
		}
		schema = avroSchema.Schema
		c.cache[x] = schema
	}

	return
}

