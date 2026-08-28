//go:build linux

package verification

import (
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/vfs"
)

func (v *verificationJob) walkDir(fs *vfs.LocalFS, entry *pxar.FileInfo, prefix string, files []fileEntry, cfg coredb.SpotCheckConfig) ([]fileEntry, error) {
	if entry.IsDir() {
		children, err := fs.ReadDir(entry.EntryRangeStart)
		if err != nil {
			return files, nil
		}
		for _, child := range children {
			childPath := prefix + "/" + child.Name()
			files, err = v.walkDir(fs, &child, childPath, files, cfg)
			if err != nil {
				return files, err
			}
		}
	} else if entry.IsFile() && len(entry.ContentRange) == 2 {
		filePath := prefix

		if !v.matchesFilters(filePath, entry, cfg) {
			return files, nil
		}

		files = append(files, fileEntry{
			Path:         filePath,
			ContentStart: entry.ContentRange[0],
			ContentEnd:   entry.ContentRange[1],
			Size:         entry.Size(),
		})
	}

	return files, nil
}

// matchesFilters checks if a file matches the spot check filter criteria.
// Exclude filters take absolute precedence: if a file matches any exclude
// non-excluded files are eligible. Otherwise the file must match at least

// matchesFilters checks if a file matches the spot check filter criteria.
// Exclude filters take absolute precedence: if a file matches any exclude
// non-excluded files are eligible. Otherwise the file must match at least
func (v *verificationJob) matchesFilters(path string, entry *pxar.FileInfo, cfg coredb.SpotCheckConfig) bool {
	if len(cfg.Filters) == 0 {
		return true
	}

	var includes, excludes []coredb.SpotCheckFilter
	for _, f := range cfg.Filters {
		if f.FilterType == "exclude" {
			excludes = append(excludes, f)
		} else {
			includes = append(includes, f)
		}
	}

	// Exclude takes absolute precedence
	for _, filter := range excludes {
		if filterMatchesFile(path, entry, filter) {
			return false
		}
	}

	if len(includes) == 0 {
		return true
	}

	// Must match at least one include filter
	for _, filter := range includes {
		if filterMatchesFile(path, entry, filter) {
			return true
		}
	}
	return false
}

func filterMatchesFile(path string, entry *pxar.FileInfo, filter coredb.SpotCheckFilter) bool {
	if filter.PathPattern != "" {
		if strings.Contains(filter.PathPattern, "*") {
			matched, err := filepath.Match(filter.PathPattern, filepath.Base(path))
			if err != nil {
				log.Error(err, "")
			}
			if !matched {
				return false
			}
		} else {
			if !strings.HasPrefix(path, filter.PathPattern) {
				return false
			}
		}
	}

	if filter.MinSize > 0 && entry.Size() < filter.MinSize {
		return false
	}
	if filter.MaxSize > 0 && entry.Size() > filter.MaxSize {
		return false
	}

	return true
}

// verifyFile verifies a single file by comparing the agent's SHA-256 hash
// of the live file against the hash of the same file extracted from the
// stored pxar archive.
