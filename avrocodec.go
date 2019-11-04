package main

import "github.com/linkedin/goavro"

type AvroCodec struct {
	csrc SchemaRegistryClient
	codecCache map[SubjectVersionID]*goavro.Codec
}

func NewAvroCodec(csrc SchemaRegistryClient) (*AvroCodec) {
	return &AvroCodec{csrc, make(map[SubjectVersionID]*goavro.Codec)}
}

func (c *AvroCodec) GetCodecFor(key SubjectVersionID) (codec *goavro.Codec, err error) {

	codec, ok := c.codecCache[key]

	if !ok {
		var schema string
		schema, err = c.csrc.GetSchemaFor(key)
		if err != nil {
			return
		}
		codec, err = goavro.NewCodec(schema)
		if err != nil {
			return
		}
		c.codecCache[key] = codec
	}

	return
}
