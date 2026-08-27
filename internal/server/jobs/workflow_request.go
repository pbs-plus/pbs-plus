//go:build linux

package jobs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

const (
	WorkflowBackup       = "backup"
	WorkflowRestore      = "restore"
	WorkflowVerification = "verification"
	WorkflowMtfMigration = "mtf.migration"
	WorkflowMtfScan      = "mtf.scan"

	WorkflowSnapshotMount   = "snapshot.mount"
	WorkflowSnapshotUnmount = "snapshot.unmount"
	WorkflowSnapshotCommit  = "snapshot.commit"
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

type SnapshotMountInput struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	BackupTime string `json:"backup_time"`
	FileName   string `json:"file_name"`
	Mode       string `json:"mode"`
	MountPath  string `json:"mount_path"`
	UPID       string `json:"upid"`
	Web        bool   `json:"web"`
}

type SnapshotUnmountInput struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	BackupTime string `json:"backup_time"`
	FileName   string `json:"file_name"`
	MountPath  string `json:"mount_path"`
	Force      bool   `json:"force"`
	UPID       string `json:"upid"`
	Web        bool   `json:"web"`
}

type SnapshotCommitInput struct {
	Datastore string `json:"datastore"`
	MountPath string `json:"mount_path"`
	UPID      string `json:"upid"`
	Web       bool   `json:"web"`
}

func NewWorkflowSubmit(kind, definitionID, trigger, dedupeKey string, payload any, resources []string, maxAttempts int, retryDelay time.Duration) (jobdb.SubmitRequest, error) {
	if dedupeKey == "" {
		id, err := NewExecutionID()
		if err != nil {
			return jobdb.SubmitRequest{}, err
		}
		dedupeKey = id
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return jobdb.SubmitRequest{}, fmt.Errorf("encoding workflow payload: %w", err)
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if retryDelay < time.Second {
		retryDelay = time.Second
	}
	return jobdb.SubmitRequest{
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
