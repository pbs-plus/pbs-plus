//go:build linux

package verification

import (
	"context"
	"fmt"
	"math"
	mrand "math/rand"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

// weightedShuffleBackups reorders backup jobs using weighted random selection
// was last successfully verified. Jobs that have never been verified receive
// the maximum weight. This ensures uniform coverage over successive runs and
// prevents the same backup job from being selected repeatedly.
func weightedShuffleBackups(backups []coredb.Backup, db *coredb.Store, verificationJobID string) []coredb.Backup {
	if len(backups) <= 1 {
		return backups
	}

	lastVerified := make(map[string]int64)
	results, err := db.GetVerificationResults(verificationJobID)
	if err == nil {
		for _, r := range results {
			if r.Status != "completed" {
				continue
			}
			parts := strings.SplitN(r.Snapshot, "/", 3)
			if len(parts) >= 2 {
				hostname := proxmox.NormalizeHostname(parts[1])
				if r.CompletedAt > lastVerified[hostname] {
					lastVerified[hostname] = r.CompletedAt
				}
			}
		}
	}

	now := time.Now().Unix()

	// Compute weights: inverse of seconds since last verification.
	// Never-verified jobs get a very large weight.
	weights := make([]float64, len(backups))
	for i, b := range backups {
		hostname := proxmox.NormalizeHostname(b.Target.GetHostname())
		last := lastVerified[hostname]
		if last == 0 {
			// Never verified  -  maximum weight
			weights[i] = float64(now)
		} else {
			elapsed := float64(now - last)
			if elapsed < 1 {
				elapsed = 1 // minimum 1 second gap to avoid zero
			}
			weights[i] = elapsed
		}
	}

	// Pick a random index proportional to weight, move it to the output,
	remaining := make([]int, len(backups))
	for i := range remaining {
		remaining[i] = i
	}

	buf, cryptoErr := crypto.SecureRandomBytes(len(backups) * 4)
	if cryptoErr != nil {
		// Fallback: just shuffle with math/rand
		mrand.Shuffle(len(backups), func(i, j int) {
			backups[i], backups[j] = backups[j], backups[i]
		})
		return backups
	}

	result := make([]coredb.Backup, 0, len(backups))
	remWeights := make([]float64, len(weights))
	copy(remWeights, weights)

	for sel := range backups {
		remTotal := 0.0
		for _, idx := range remaining[sel:] {
			remTotal += remWeights[idx]
		}
		if remTotal <= 0 {
			remTotal = 1
		}

		// Pick a random threshold using crypto/rand
		raw := uint32(buf[sel*4]) | uint32(buf[sel*4+1])<<8 | uint32(buf[sel*4+2])<<16 | uint32(buf[sel*4+3])<<24
		threshold := float64(raw%1_000_000) / 1_000_000.0 * remTotal

		cumulative := 0.0
		chosen := remaining[sel]
		for _, idx := range remaining[sel:] {
			cumulative += remWeights[idx]
			if cumulative >= threshold {
				chosen = idx
				break
			}
		}

		for j := sel; j < len(remaining); j++ {
			if remaining[j] == chosen {
				remaining[sel], remaining[j] = remaining[j], remaining[sel]
				break
			}
		}
		result = append(result, backups[chosen])
	}

	return result
}

// sampleFiles walks the pxar archive to enumerate files, then returns a sample
// based on the configured strategy (random, systematic, or stratified).
func (v *verificationJob) sampleFiles(ctx context.Context, job coredb.VerificationJob, vs *verifyState, snap *snapshotInfo) ([]fileEntry, error) {
	root, err := vs.fs.Root()
	if err != nil {
		return nil, fmt.Errorf("failed to get archive root: %w", err)
	}

	var allFiles []fileEntry
	allFiles, err = v.walkDir(vs.fs, root, "", allFiles, job.SpotConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to walk archive: %w", err)
	}

	if len(allFiles) == 0 {
		return nil, ErrNoFilesToVerify
	}

	v.mu.Lock()
	v.totalPopulation = len(allFiles)
	v.mu.Unlock()

	sampleCount := job.SpotConfig.SampleCount
	if job.SpotConfig.SampleCountPercent > 0 {
		sampleCount = int(math.Ceil(float64(len(allFiles)) * job.SpotConfig.SampleCountPercent / 100))
	}
	if sampleCount <= 0 {
		sampleCount = 10
	}
	if sampleCount > len(allFiles) {
		sampleCount = len(allFiles)
	}

	strategy := job.SpotConfig.SamplingStrategy
	if strategy == "" {
		strategy = "random"
	}

	switch strategy {
	case "systematic":
		return systematicSample(allFiles, sampleCount), nil
	case "stratified":
		return stratifiedSample(allFiles, sampleCount), nil
	default: // random
		mrand.Shuffle(len(allFiles), func(i, j int) {
			allFiles[i], allFiles[j] = allFiles[j], allFiles[i]
		})
		return allFiles[:sampleCount], nil
	}
}

func systematicSample(files []fileEntry, n int) []fileEntry {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	if n >= len(files) {
		return files
	}

	result := make([]fileEntry, n)
	step := float64(len(files)) / float64(n)
	for i := range n {
		idx := int(float64(i) * step)
		result[i] = files[idx]
	}
	return result
}

func stratifiedSample(files []fileEntry, n int) []fileEntry {
	if n >= len(files) {
		return files
	}

	groups := make(map[string][]fileEntry)
	for _, f := range files {
		dir := topLevelDir(f.Path)
		groups[dir] = append(groups[dir], f)
	}

	var result []fileEntry
	remaining := n
	groupNames := make([]string, 0, len(groups))
	for k := range groups {
		groupNames = append(groupNames, k)
	}
	sort.Strings(groupNames)

	for i, name := range groupNames {
		g := groups[name]
		var allocated int
		if i == len(groupNames)-1 {
			allocated = remaining
		} else {
			allocated = min(int(math.Round(float64(len(g))/float64(len(files))*float64(n))), remaining)
		}

		if allocated > len(g) {
			allocated = len(g)
		}

		mrand.Shuffle(len(g), func(i, j int) {
			g[i], g[j] = g[j], g[i]
		})

		result = append(result, g[:allocated]...)
		remaining -= allocated
	}

	return result
}

// topLevelDir extracts the top-level directory from a path (e.g. "/data/file.txt" → "/data").

// topLevelDir extracts the top-level directory from a path (e.g. "/data/file.txt" → "/data").
func topLevelDir(path string) string {
	path = strings.TrimPrefix(path, "/")
	before, _, ok := strings.Cut(path, "/")
	if !ok {
		return "/"
	}
	return before
}
