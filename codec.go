package gokafkaavro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
)

// Codec decodes kafka avro messages using a schema registry
type Codec struct {
	client     schemaRegistryClient
	codecCache map[subjectVersionID]*goavro.Codec
}

// NewCodec returns a new instance of Codec
func NewCodec(client schemaRegistryClient) (*Codec) {
	return &Codec{client, make(map[subjectVersionID]*goavro.Codec)}
}

// Decode builds a native go interface from the given avro data
func (c *Codec) Decode(topic string, isKey bool, data []byte) (native interface{}, newBuf []byte, err error) {

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

