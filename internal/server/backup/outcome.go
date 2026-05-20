//go:build linux

package backup

// BackupStatus represents the final status of a backup job.
type BackupStatus string

const (
	StatusOK       BackupStatus = "ok"
	StatusWarnings BackupStatus = "warnings"
	StatusFailed   BackupStatus = "failed"
	StatusCanceled BackupStatus = "canceled"
)

// String implements fmt.Stringer.
func (s BackupStatus) String() string {
	return string(s)
}

// IsSuccess returns true for ok or warnings statuses.
func (s BackupStatus) IsSuccess() bool {
	return s == StatusOK || s == StatusWarnings
}
