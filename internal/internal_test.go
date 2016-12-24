package internal

import (
	"testing"

	"github.com/MasterOfBinary/redistypes/internal/test"
)

func TestPrependInterface(t *testing.T) {
	scenarios := []struct {
		name  string
		first interface{}
		args  []string
	}{
		{
			name:  "simple",
			first: "abc",
			args:  []string{"Hello", "world"},
		},
		{
			name:  "empty args",
			first: "abc",
			args:  []string{},
		},
		{
			name:  "long string",
			first: "abc",
			args:  []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
		},
		{
			name:  "first is nil",
			first: nil,
			args:  []string{"Hello", "world"},
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario // Capture variable
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()

			args := test.StringSliceToInterfaceSlice(scenario.args)

			got := PrependInterface(scenario.first, args...)
			if len(got) != 1+len(scenario.args) {
				t.Errorf("Invalid length, want: %v, got: %v", len(got), 1+len(scenario.args))
			}

			if got[0] != scenario.first {
				t.Errorf("Invalid first item, want: %v, got: %v", scenario.first, got[0])
			}

			for i, item := range got[1:] {
				if item != scenario.args[i] {
					t.Errorf("Invalid item %v, want: %v, got: %v", i, scenario.args[i], item)
				}
			}
		})
	}
}
