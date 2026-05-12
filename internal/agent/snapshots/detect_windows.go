//go:build windows

package snapshots

// detectFilesystem on Windows always returns "ntfs".
func detectFilesystem(path string) (string, error) {
	return "ntfs", nil
}
