package main

import (
	"testing"
)

func TestParseFloat(t *testing.T) {
	var tests = []struct {
		Name  string
		Input string
		Want  int32
	}{
		{
			Name:  "positive",
			Input: "154.1",
			Want:  1541,
		},
		{
			Name:  "negative",
			Input: "-154.1",
			Want:  -1541,
		},
		{
			Name:  "zero",
			Input: "0.0",
			Want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			r := fastFloatParse([]byte(tt.Input))

			if r != tt.Want {
				t.Errorf("failed to parse float - got %d, want %d", r, tt.Want)
			}

		})
	}
}
