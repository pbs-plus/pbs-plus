package api

import (
	"fmt"
	"math"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

func HumanReadableBytes(bytes int) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	b := float64(bytes)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TiB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2f GiB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2f MiB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2f KiB", b/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func HumanReadableSpeed(bytesPerSec int) string {
	const (
		KB = 1024.0
		MB = KB * 1024
		GB = MB * 1024
	)
	s := float64(bytesPerSec)
	switch {
	case s >= GB:
		return fmt.Sprintf("%.2f GiB/s", s/GB)
	case s >= MB:
		return fmt.Sprintf("%.2f MiB/s", s/MB)
	case s >= KB:
		return fmt.Sprintf("%.2f KiB/s", s/KB)
	default:
		return fmt.Sprintf("%.2f B/s", s)
	}
}

type ParsedTaskStatus struct {
	Category string `json:"category"` // "ok", "error", "warning", "unknown", "queued"
	Icon     string `json:"icon"`     // CSS class suffix, e.g. "check good"
	Text     string `json:"text"`     // Display text
}

func ParseTaskStatus(status string) ParsedTaskStatus {
	if status == "" {
		return ParsedTaskStatus{}
	}

	if status == "OK" {
		return ParsedTaskStatus{
			Category: "ok",
			Icon:     "check good",
			Text:     "OK",
		}
	}

	if status == "unknown" {
		return ParsedTaskStatus{
			Category: "unknown",
			Icon:     "question faded",
			Text:     "unknown",
		}
	}

	if len(status) > 9 && status[:9] == "WARNINGS:" {
		return ParsedTaskStatus{
			Category: "warning",
			Icon:     "exclamation warning",
			Text:     status,
		}
	}

	if len(status) > 7 && status[:7] == "QUEUED:" {
		return ParsedTaskStatus{
			Category: "queued",
			Icon:     "tasks faded",
			Text:     status,
		}
	}

	return ParsedTaskStatus{
		Category: "error",
		Icon:     "times critical",
		Text:     "Error: " + status,
	}
}

func FormatDuration(seconds int64) string {
	if seconds <= 0 {
		return ""
	}
	d := float64(seconds)
	switch {
	case d < 60:
		return fmt.Sprintf("%ds", seconds)
	case d < 3600:
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	default:
		h := seconds / 3600
		m := (seconds % 3600) / 60
		return fmt.Sprintf("%dh %dm", h, m)
	}
}

type ConfidenceInfo struct {
	Confidence95 float64 `json:"c95"` // percentage 0-100
	Confidence99 float64 `json:"c99"` // percentage 0-100
}

// Uses the Rule of Three for zero-failure samples and the Wilson score interval otherwise.
func ComputeConfidence(population, sample, failures int) ConfidenceInfo {
	if sample <= 0 || failures >= sample {
		return ConfidenceInfo{}
	}

	n := float64(sample)
	N := float64(population)
	if N <= 0 || n > N {
		N = n
	}

	fHat := float64(failures) / n

	var c95, c99 float64

	if failures == 0 {
		fpc := math.Sqrt((N - n) / N)
		if fpc < 0 {
			fpc = 0
		}
		c95 = clamp01(1 - 3.0/n*fpc)
		c99 = clamp01(1 - 4.6/n*fpc)
	} else {
		pHat := 1 - fHat
		c95 = wilsonLower(pHat, n, 1.96)
		c99 = wilsonLower(pHat, n, 2.576)
	}

	return ConfidenceInfo{
		Confidence95: math.Round(c95*1000) / 10, // round to 1 decimal (percentage)
		Confidence99: math.Round(c99*1000) / 10,
	}
}

func wilsonLower(pHat, n, z float64) float64 {
	if n <= 0 {
		return 0
	}
	denom := 1 + z*z/n
	centre := pHat + z*z/(2*n)
	spread := z * math.Sqrt(pHat*(1-pHat)/n+z*z/(4*n*n))
	lower := (centre - spread) / denom
	return clamp01(lower)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func ComputeAggregate(results []database.VerificationResult) VerificationAggregate {
	var agg VerificationAggregate
	thirtyDaysAgo := time.Now().Unix() - 30*24*3600

	for _, r := range results {
		if r.Status == "" || r.Status == "pending" {
			continue // skip incomplete runs
		}
		agg.TotalRuns++
		agg.TotalFiles += r.TotalFiles
		agg.TotalFailed += r.FailedFiles
		agg.TotalSkipped += r.SkippedFiles

		if r.FailedFiles == 0 && r.Status == "OK" {
			agg.CleanRuns++
		} else if r.FailedFiles > 0 {
			agg.FailedRuns++
		}

		if r.StartedAt >= thirtyDaysAgo {
			agg.Last30Days++
		}
	}

	if agg.TotalFiles > 0 {
		agg.PassRate = float64(agg.TotalFiles-agg.TotalFailed) / float64(agg.TotalFiles) * 100
	}

	agg.Confidence = ComputeConfidence(0, agg.TotalFiles, agg.TotalFailed).Confidence95

	return agg
}

func FormatSpeed(speed int) string {
	return fmt.Sprintf("%.2f files/s", float64(speed))
}
