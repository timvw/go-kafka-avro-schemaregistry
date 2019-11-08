package gokafkaavro

import (
	"errors"
	"fmt"
	schemaregistry "github.com/lensesio/schema-registry"
)

// schemaRegistryClient represents a client to a schema registry server
type schemaRegistryClient interface {
	GetSchemaFor(x subjectVersionID) (schema string, err error)
	GetVersionFor(subject string, schema string) (versionID int, err error)
	//Register(subject string, schema string) (versionID int, err error)
}

// CachedSchemaRegistryClient is an real schema registry client which uses a cache for lookups
type CachedSchemaRegistryClient struct {
	client *schemaregistry.Client
	cache  map[subjectVersionID]string
}

// NewCachedSchemaRegistryClient returns a new instance of a CachedSchemaRegistryClient
func NewCachedSchemaRegistryClient(client *schemaregistry.Client) (*CachedSchemaRegistryClient) {
	return &CachedSchemaRegistryClient{client, make(map[subjectVersionID]string)}
}

// GetSchemaFor returns the schema for the given subject/version
func (c *CachedSchemaRegistryClient) GetSchemaFor(x subjectVersionID) (schema string, err error) {

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

// GetVersionFor returns the version for the given subject and schema
func (c *CachedSchemaRegistryClient) GetVersionFor(subject string, schema string) (versionID int, err error) {

	isRegistered, fetchedSchema, err := c.client.IsRegistered(subject, schema)

	if err != nil {
		return
	}

	if !isRegistered {
		err = errors.New(fmt.Sprintf("the given schema is not registered for the subject %v", subject))
		return
	}

	versionID = fetchedSchema.Version
	return
}
