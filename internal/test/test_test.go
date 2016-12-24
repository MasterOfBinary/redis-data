package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSliceToInterfaceSlice(t *testing.T) {
	scenarios := []struct {
		name  string
		input []string
	}{
		{
			name:  "simple test",
			input: []string{"abc", "def", "ghi"},
		},
		{
			name:  "empty slice",
			input: []string{},
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()

			got := StringsToInterfaceSlice(scenario.input...)

			assert.Equal(t, len(scenario.input), len(got))

			for i, item := range got {
				assert.Equal(t, scenario.input[i], item)
			}
		})
	}
}
