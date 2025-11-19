package types

import "time"

// Host represents a record in the new 'hosts' table.
type Host struct {
	Hostname        string    `json:"hostname"`
	IPAddress       *string   `json:"ip_address,omitempty"` // New nullable column
	OperatingSystem string    `json:"operating_system"`
	Auth            *string   `json:"auth,omitempty"`       // Nullable
	TokenUsed       *string   `json:"token_used,omitempty"` // Nullable
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Target represents a record in the new 'targets' table.
type Target struct {
	ID              int       `json:"id"`
	Name            string    `json:"name"`                    // Simplified label (e.g., "C", "Root")
	HostHostname    *string   `json:"host_hostname,omitempty"` // Foreign key to hosts table (nullable for S3)
	MountPoint      *string   `json:"mount_point,omitempty"`   // e.g., "C:", "/", "/mnt/data" (nullable for S3)
	Path            *string   `json:"path,omitempty"`          // Original path for non-agent targets, NULL for agent targets
	TargetType      string    `json:"target_type"`             // e.g., 'agent_drive', 'agent_root', 's3'
	MountScript     *string   `json:"mount_script,omitempty"`
	DriveType       *string   `json:"drive_type,omitempty"`
	DriveName       *string   `json:"drive_name,omitempty"`
	DriveFS         *string   `json:"drive_fs,omitempty"`
	DriveTotalBytes *int64    `json:"drive_total_bytes,omitempty"` // Use int64 for bytes
	DriveUsedBytes  *int64    `json:"drive_used_bytes,omitempty"`  // Use int64 for bytes
	DriveFreeBytes  *int64    `json:"drive_free_bytes,omitempty"`  // Use int64 for bytes
	DriveTotal      *string   `json:"drive_total,omitempty"`
	DriveUsed       *string   `json:"drive_used,omitempty"`
	DriveFree       *string   `json:"drive_free,omitempty"`
	SecretS3        *string   `json:"secret_s3,omitempty"` // Nullable, only for 's3' target_type
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Host            *Host     `json:"host,omitempty"` // Embedded Host struct
	JobCount        int       `json:"job_count"`
}
