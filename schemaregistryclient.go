package gokafkaavro

import (
	"fmt"
	schemaregistry "github.com/lensesio/schema-registry"
)

// schemaRegistryClient represents a client to a schema registry server
type schemaRegistryClient interface {
	GetSchemaFor(x subjectVersionID) (schema string, err error)
}

// MockSchemaRegistryClient is a fake schemaregistryclient for testing purposes
type MockSchemaRegistryClient struct {
	cache  map[subjectVersionID]string
}

// NewMockSchemaRegistryClient returns a new instance of a MockSchemaRegistryClient
func NewMockSchemaRegistryClient(client *schemaregistry.Client) (instance *MockSchemaRegistryClient) {
	return &MockSchemaRegistryClient{make(map[subjectVersionID]string)}
}

// GetSchemaFor returns the schema for the given subject/version
func (c *MockSchemaRegistryClient) GetSchemaFor(x subjectVersionID) (schema string, err error) {

	schema, ok := c.cache[x]

	if !ok {
		err = fmt.Errorf("Could not find entry for %v", x)
	}

	return
}

// Register add a schema entry for the given subject/version
func (c *MockSchemaRegistryClient) Register(subject string, versionID int, schema string) {
	key := subjectVersionID{subject, versionID}
	c.cache[key] = schema
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
