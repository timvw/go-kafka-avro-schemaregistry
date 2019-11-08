package gokafkaavro

import (
	"testing"
)

func TestGetSchemaID(t *testing.T) {

	var tests = []struct {
		input []byte
		want  int
	}{
		{[]byte{0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 2}, 2},
		{[]byte{0, 0, 1, 0}, 256},
	}

	for _, test := range tests {
		if got := getSchemaID(test.input); got != test.want {
			t.Errorf("readUint32(%v) returned %d, want %d", test.input, got, test.want)
		}
	}
}

func TestBytesForSchemaID(t *testing.T) {

	var tests = []struct {
		want []byte
		input  int
	}{
		{[]byte{0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 2}, 2},
		{[]byte{0, 0, 1, 0}, 256},
	}

	for _, test := range tests {
		if got := bytesForSchemaID(test.input); string(got) != string(test.want) {
			t.Errorf("readUint32(%v) returned %d, want %d", test.input, got, test.want)
		}
	}
}