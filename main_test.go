package main

import "testing"

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
		if got := Codec.getSchemaID(test.input); got != test.want {
			t.Errorf("readUint32(%v) returned %d, want %d", test.input, got, test.want)
		}
	}

}
