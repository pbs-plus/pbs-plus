//go:build linux

package jobs

import "testing"

func TestClearRunFinalizedLetsARetriedExecutionRunAgain(t *testing.T) {
	const jobID, executionID = "DP-D030-D", "exec-1"

	MarkRunFinalized(jobID, executionID)
	if !RunFinalized(jobID, executionID) {
		t.Fatal("a terminal write should mark the run finalized")
	}

	ClearRunFinalized(jobID, executionID)
	if RunFinalized(jobID, executionID) {
		t.Fatal("a retry of the same execution must be able to record itself as running")
	}
}

func TestClearRunFinalizedLeavesAnotherExecutionAlone(t *testing.T) {
	const jobID = "DP-D024-D"

	MarkRunFinalized(jobID, "exec-current")
	ClearRunFinalized(jobID, "exec-stale")

	if !RunFinalized(jobID, "exec-current") {
		t.Fatal("a stale clear must not reopen the run owned by a different execution")
	}
}
