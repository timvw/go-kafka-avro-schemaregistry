package gokafkaavro

import (
	"encoding/binary"
	"errors"
	"fmt"
	schemaregistry "github.com/lensesio/schema-registry"
	"github.com/linkedin/goavro"
)

type SubjectName = string
type AvroSchema = string
type SubjectVersion = int

type SubjectNameStrategy interface {
	GetSubjectName(topic string, isKey bool)(subjectName SubjectName)
}

type TopicNameStrategy struct {
}

func (ts TopicNameStrategy) GetSubjectName(topic string, isKey bool)(subjectName SubjectName) {
	if isKey {
		subjectName = fmt.Sprintf("%v-key", topic)
	} else {
		subjectName = fmt.Sprintf("%v-value", topic)
	}
	return
}

type Decoder struct {
	client schemaregistry.Client
	subjectName SubjectName
	codecByVersion map[SubjectVersion]goavro.Codec
}

func NewDecoder(client schemaregistry.Client, subjectName SubjectName)(decoder Decoder, err error) {
	codecByVersion := make(map[SubjectVersion]goavro.Codec)
	decoder = Decoder{client,subjectName, codecByVersion}
	return
}

func (d Decoder) Decode(data []byte) (native interface{}, err error) {

	magicByte := data[0]

	if magicByte != 0 {
		err = errors.New("Unknown magic byte")
		return
	}

	subjectVersion := int(binary.BigEndian.Uint32((data[1:5])))

	codec, found := d.codecByVersion[subjectVersion]
	if !found {

		schema, clientErr := d.client.GetSchemaBySubject(d.subjectName, subjectVersion)
		if clientErr != nil {
			err = clientErr
			return
		}

		codecPtr, avroErr := goavro.NewCodec(schema.Schema)
		if avroErr != nil {
			err = avroErr
			return
		}

		codec = *codecPtr
		d.codecByVersion[subjectVersion] = codec
	}

	native, _, err = codec.NativeFromBinary(data[5:])
	return
}

type Encoder struct {
	headerBytes []byte
	codec goavro.Codec
}

func NewEncoder(client schemaregistry.Client, autoRegister bool, subjectName SubjectName, avroSchema AvroSchema)(encoder Encoder, err error) {

	var subjectVersion SubjectVersion

	if(autoRegister) {
		subjectVersion, err = client.RegisterNewSchema(subjectName, avroSchema)
		if err != nil {
			return
		}
	} else {
		isRegistered, schema, clientErr := client.IsRegistered(subjectName, avroSchema)
		if clientErr != nil {
			err = clientErr
			return
		}
		if !isRegistered {
			err = errors.New(fmt.Sprintf("There is no registration on subject %v for schema %v", subjectName, avroSchema))
			return
		}

		subjectVersion = schema.Version
	}

	headerBytes := make([]byte, 5) // 5 bytes, first byte is the magic byte with value 0
	binary.BigEndian.PutUint32(headerBytes[1:], uint32(subjectVersion)) // the next 4 bytes are the subject version

	codec, codecErr := goavro.NewCodec(avroSchema)
	if codecErr != nil {
		err = codecErr
		return
	}

	encoder = Encoder{ headerBytes, *codec}
	return
}

func (e Encoder) Encode(native interface{})(avroBytes []byte, err error) {
	dataBytes, err := e.codec.BinaryFromNative(nil, native)
	avroBytes = append(e.headerBytes, dataBytes...)
	return
}

