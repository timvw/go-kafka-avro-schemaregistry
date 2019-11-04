package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

type AvroCodec struct {
	csrc SchemaRegistryClient
	codecCache map[SubjectVersionID]*goavro.Codec
}

func NewAvroCodec(csrc SchemaRegistryClient) (*AvroCodec) {
	return &AvroCodec{csrc, make(map[SubjectVersionID]*goavro.Codec)}
}

func (c *AvroCodec) GetCodecFor(subjectVersion SubjectVersionID) (codec *goavro.Codec, err error) {

	codec, ok := c.codecCache[subjectVersion]

	if !ok {
		var schema string
		schema, err = c.csrc.GetSchemaFor(subjectVersion)
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

func (c *AvroCodec) DecodeValue(e *kafka.Message) (native interface{}, newBuf []byte, err error) {

	subjectVersion, err := extractSubjectAndVersionFromValue(e)
	if err != nil {
		return
	}

	codec, err := c.GetCodecFor(subjectVersion)
	if err != nil {
		return
	}

	data := e.Value[5:]
	return codec.NativeFromBinary(data)
}

func (c *AvroCodec) DecodeKey(e *kafka.Message) (native interface{}, newBuf []byte, err error) {

	subjectVersion, err := extractSubjectAndVersionFromKey(e)
	if err != nil {
		return
	}

	codec, err := c.GetCodecFor(subjectVersion)
	if err != nil {
		return
	}

	data := e.Key[5:]
	return codec.NativeFromBinary(data)
}
