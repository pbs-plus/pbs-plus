//go:build linux

package backup

import (
	"errors"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func TestValidateAgentConnection(t *testing.T) {
	target := coredb.Target{Name: "agent-target"}

	if err := validateAgentConnection(target, true); err != nil {
		t.Fatalf("online agent rejected: %v", err)
	}

	err := validateAgentConnection(target, false)
	if !errors.Is(err, jobs.ErrTargetUnreachable) {
		t.Fatalf("offline agent error = %v, want ErrTargetUnreachable", err)
	}
}
