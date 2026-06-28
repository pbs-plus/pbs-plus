package mtf

import "maps"

import "sync"

type ProgressSnapshot struct {
	Files      int64   `json:"files"`
	Dirs       int64   `json:"dirs"`
	Bytes      int64   `json:"bytes"`
	PhysInst   float64 `json:"phys_inst"`
	PhysAvg    float64 `json:"phys_avg"`
	TapeInst   float64 `json:"tape_inst"`
	TapeAvg    float64 `json:"tape_avg"`
	IngestInst float64 `json:"ingest_inst"`
	IngestAvg  float64 `json:"ingest_avg"`
	UpdatedAt  int64   `json:"updated_at"`
}

var (
	progressMu      sync.RWMutex
	progressByJobID = map[string]ProgressSnapshot{}
)

func PublishProgress(jobID string, p ProgressSnapshot) {
	progressMu.Lock()
	progressByJobID[jobID] = p
	progressMu.Unlock()
}

func ClearProgress(jobID string) {
	progressMu.Lock()
	delete(progressByJobID, jobID)
	progressMu.Unlock()
}

func ProgressFor(jobID string) (ProgressSnapshot, bool) {
	progressMu.RLock()
	p, ok := progressByJobID[jobID]
	progressMu.RUnlock()
	return p, ok
}

func AllProgress() map[string]ProgressSnapshot {
	progressMu.RLock()
	defer progressMu.RUnlock()
	out := make(map[string]ProgressSnapshot, len(progressByJobID))
	maps.Copy(out, progressByJobID)
	return out
}
