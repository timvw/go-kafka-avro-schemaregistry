package gokafkaavro

import (
	"fmt"
	schemaregistry "github.com/lensesio/schema-registry"
	"testing"
)

func TestSchemaRegistry(t *testing.T) {

	schemaRegistryURL := "http://localhost:8081"

	client, err := schemaregistry.NewClient(schemaRegistryURL)
	if err != nil {
		return
	}

	subject := "test-value"

	schema, err := client.GetSchemaBySubject(subject, 1)
	schemaStr := schema.Schema

	fmt.Printf("found schema: %v", schemaStr)
	fmt.Println()

	isRegistered, schema2, err := client.IsRegistered(subject, schemaStr)
	fmt.Printf("the schema is registered: %v, %v", isRegistered, schema2)

}
