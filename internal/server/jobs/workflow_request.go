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
	WorkflowSnapshotInit    = "snapshot.init"
	WorkflowSnapshotCompose = "snapshot.compose"
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
	Backend    string `json:"backend"`
	Outpost    string `json:"outpost,omitempty"`
	ShareName  string `json:"share_name,omitempty"`
	SubPath    string `json:"sub_path,omitempty"`
	Profile    string `json:"profile,omitempty"`
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
	Reason     string `json:"reason,omitempty"`
	UPID       string `json:"upid"`
	Web        bool   `json:"web"`
}

type SnapshotCommitInput struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	BackupTime string `json:"backup_time"`
	MountPath  string `json:"mount_path"`
	UPID       string `json:"upid"`
	Web        bool   `json:"web"`
}

type SnapshotInitInput struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	Backend    string `json:"backend"`
	Outpost    string `json:"outpost,omitempty"`
	MountPath  string `json:"mount_path"`
	UPID       string `json:"upid"`
	Web        bool   `json:"web"`
}

type SnapshotComposeInput struct {
	Datastore  string   `json:"datastore"`
	SourceNS   string   `json:"source_ns"`
	SourceType string   `json:"source_type"`
	SourceID   string   `json:"source_id"`
	SourceTime string   `json:"source_time"`
	SourceFile string   `json:"source_file"`
	TargetNS   string   `json:"target_ns"`
	TargetType string   `json:"target_type"`
	TargetID   string   `json:"target_id"`
	Paths      []string `json:"paths"`
	StripRoot  bool     `json:"strip_root"`
	UPID       string   `json:"upid"`
	Web        bool     `json:"web"`
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
