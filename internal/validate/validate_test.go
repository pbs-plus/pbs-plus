package validate

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func TestDefaultExclusionsPassValidation(t *testing.T) {
	for _, path := range conf.DefaultExclusions {
		if err := ValidateExclusionPath(path); err != nil {
			t.Errorf("default exclusion %q fails validation: %v", path, err)
		}
	}
}
