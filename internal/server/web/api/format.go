package api

import (
	"fmt"
	"math"
)

// HumanReadableBytes converts bytes to a human-readable string (e.g. "1.50 GiB").
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

// HumanReadableSpeed converts bytes/sec to a human-readable string (e.g. "1.50 GiB/s").
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

// ParsedTaskStatus represents a parsed task status with display info.
type ParsedTaskStatus struct {
	Category string `json:"category"` // "ok", "error", "warning", "unknown", "queued"
	Icon     string `json:"icon"`     // CSS class suffix, e.g. "check good"
	Text     string `json:"text"`     // Display text
}

// ParseTaskStatus parses a task status string and returns display information.
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

// FormatDuration formats seconds into a human-readable duration string.
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

// ComputeConfidenceJSON returns confidence info suitable for JSON responses.
type ConfidenceInfo struct {
	Confidence95 float64 `json:"c95"` // percentage 0-100
	Confidence99 float64 `json:"c99"` // percentage 0-100
}

// ComputeConfidence computes statistical confidence for verification results.
func ComputeConfidence(population, sample, failures int) ConfidenceInfo {
	c95, c99 := computeConfidence(population, sample, failures)
	return ConfidenceInfo{
		Confidence95: math.Round(c95*1000) / 10, // round to 1 decimal
		Confidence99: math.Round(c99*1000) / 10,
	}
}

// FormatSpeed returns "X.XX files/s" string.
func FormatSpeed(speed int) string {
	return fmt.Sprintf("%.2f files/s", float64(speed))
}
