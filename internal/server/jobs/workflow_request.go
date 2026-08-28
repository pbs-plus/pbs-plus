//go:build linux

package jobs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/store"
)

const (
	WorkflowBackup       = "backup"
	WorkflowRestore      = "restore"
	WorkflowVerification = "verification"
	WorkflowMtfMigration = "mtf.migration"
	WorkflowMtfScan      = "mtf.scan"
)

type BackupInput struct {
	SkipCheck       bool     `json:"skip_check"`
	Web             bool     `json:"web"`
	ExtraExclusions []string `json:"extra_exclusions"`
}

type RestoreInput struct {
	SkipCheck bool `json:"skip_check"`
	Web       bool `json:"web"`
}

type VerificationInput struct {
	Web bool `json:"web"`
}

type MtfScanInput struct {
	TapeDevice    string   `json:"tape_device"`
	ChangerDevice string   `json:"changer_device"`
	DriveIndex    int      `json:"drive_index"`
	BKFPath       string   `json:"bkf_path"`
	Label         string   `json:"label"`
	Barcodes      []string `json:"barcodes"`
}

func NewWorkflowSubmit(kind, definitionID, trigger, dedupeKey string, payload any, resources []string, maxAttempts int, retryDelay time.Duration) (store.SubmitRequest, error) {
	if dedupeKey == "" {
		id, err := NewExecutionID()
		if err != nil {
			return store.SubmitRequest{}, err
		}
		dedupeKey = id
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return store.SubmitRequest{}, fmt.Errorf("encoding workflow payload: %w", err)
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if retryDelay < time.Second {
		retryDelay = time.Second
	}
	return store.SubmitRequest{
		Kind:              kind,
		DefinitionID:      definitionID,
		Trigger:           trigger,
		DedupeKey:         dedupeKey,
		Payload:           encoded,
		Resources:         resources,
		MaxAttempts:       maxAttempts,
		RetryInitialDelay: retryDelay,
		RetryMaxDelay:     retryDelay,
		RunAt:             time.Now(),
	}, nil
}
