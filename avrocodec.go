package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/linkedin/goavro"
)

type SubjectVersionID struct {
	subject   string
	versionID int
}

type AvroCodec struct {
	schemaRegistryClient SchemaRegistryClient
	codecCache           map[SubjectVersionID]*goavro.Codec
}

func NewAvroCodec(schemaRegistryClient *schemaregistry.Client) (*AvroCodec) {
	cachedSchemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryClient)
	return &AvroCodec{cachedSchemaRegistryClient, make(map[SubjectVersionID]*goavro.Codec)}
}

func (c *AvroCodec) DecodeValue(msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.decode(*msg.TopicPartition.Topic, false, msg.Value)
}

func (c *AvroCodec) DecodeKey(msg *kafka.Message) (native interface{}, newBuf []byte, err error) {
	return c.decode(*msg.TopicPartition.Topic, true, msg.Key)
}

func (c *AvroCodec) decode(topic string, isKey bool, data []byte) (native interface{}, newBuf []byte, err error) {

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

func extractSubjectAndVersionFromData(topic string, isKey bool, data []byte) (key SubjectVersionID, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	subject := getSubject(topic, isKey)
	versionID := getSchemaID(data[1:5])
	key = SubjectVersionID{subject, versionID}
	return
}

func getSubject(topic string, isKey bool) (subject string) {
	if isKey {
		return fmt.Sprintf("%v-key", topic)
	} else {
		return fmt.Sprintf("%v-value", topic)
	}
}

func getSchemaID(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
}

func (c *AvroCodec) getCodecFor(subjectVersion SubjectVersionID) (codec *goavro.Codec, err error) {

	codec, ok := c.codecCache[subjectVersion]

	if !ok {
		var schema string
		schema, err = c.schemaRegistryClient.GetSchemaFor(subjectVersion)
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


