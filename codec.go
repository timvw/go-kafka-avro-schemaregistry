package gokafkaavro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
)

// Codec decodes kafka avro messages using a schema registry
type Codec struct {
	client              schemaRegistryClient
	codecCache          map[subjectVersionID]*goavro.Codec
	subjectNameStrategy func(topic string, isKey bool)(string)
}

// NewCodec returns a new instance of Codec
func NewCodec(client schemaRegistryClient) (*Codec) {
	return &Codec{client, make(map[subjectVersionID]*goavro.Codec), getTopicNameStrategy}
}

// Decode returns a native datum value for the binary encoded byte slice
// in accordance with the Avro schema attached to the data
// [wire-format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format).
// On success, it returns the decoded datum and a nil error value.
// On error, it returns nil for the datum value and the error message.
func (c *Codec) Decode(topic string, isKey bool, data []byte) (native interface{}, err error) {

	subjectVersion, err := extractSubjectAndVersionFromData(topic, isKey, data)
	if err != nil {
		return
	}

	codec, err := c.getCodecFor(subjectVersion)
	if err != nil {
		return
	}

	native, _, err = codec.NativeFromBinary(data[5:])
	return
}

// SubjectNameStrategy represents the actual method to resolve a subject name
type SubjectNameStrategy interface {
	GetSubjectName(topic string, isKey bool, data []byte)
}

type subjectVersionID struct {
	subject   string
	versionID int
}

func extractSubjectAndVersionFromData(topic string, isKey bool, data []byte) (key subjectVersionID, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	subject := getTopicNameStrategy(topic, isKey)
	versionID := getSchemaID(data[1:5])
	key = subjectVersionID{subject, versionID}
	return
}

func getTopicNameStrategy(topic string, isKey bool) (subject string) {
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
		schema, err = c.client.GetSchemaFor(subjectVersion)
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

