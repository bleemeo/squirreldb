package main

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestLabelsToText(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input          []prompb.Label
		expectedOutput string
	}{
		{
			input: []prompb.Label{
				{
					Name:  "__name__",
					Value: "cpu_used",
				},
				{
					Name:  "item",
					Value: "cpu-7",
				},
			},
			expectedOutput: `__name__="cpu_used",item="cpu-7"`,
		},
		{
			input: []prompb.Label{
				{
					Name:  "__name__",
					Value: "ops",
				},
				{
					Name:  "instance",
					Value: `srv-"g"`,
				},
			},
			expectedOutput: `__name__="ops",instance="srv-\"g\""`,
		},
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			output := labelsToText(tc.input)
			if output != tc.expectedOutput {
				t.Fatalf("expected: %s, got: %s", tc.expectedOutput, output)
			}
		})
	}
}
