package main

import schemaregistry "github.com/lensesio/schema-registry"

type SchemaRegistryClient interface {
	GetSchemaFor(x SubjectVersionID) (schema string, err error)
}

type CachedSchemaRegistryClient struct {
	client *schemaregistry.Client
	cache  map[SubjectVersionID]string
}

// NewCachedSchemaRegistryClient returns a schemaregistryclient which memoizes succesful fetches
func NewCachedSchemaRegistryClient(client *schemaregistry.Client) (instance *CachedSchemaRegistryClient) {
	return &CachedSchemaRegistryClient{client, make(map[SubjectVersionID]string)}
}

// GetSchemaBySubject fetches the avro schema for the given subject and schemaID
func (c *CachedSchemaRegistryClient) GetSchemaFor(x SubjectVersionID) (schema string, err error) {

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
