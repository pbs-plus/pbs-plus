//go:build unix

package pxar

// prepareRestoreProcess is a no-op on Unix. On Windows it enables the
// backup/restore/owner privileges in the process token so metadata (owner,
// group, DACL) can be written on objects the restore user does not own.
func prepareRestoreProcess() {}
