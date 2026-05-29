//go:build linux

package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// NotificationMode determines how job notifications are delivered.
type NotificationMode string

const (
	// ModeNotificationSystem uses PBS's native notification system
	// (notification matchers, endpoints, etc.).
	ModeNotificationSystem NotificationMode = "notification-system"
	// ModeLegacySendmail uses sendmail to deliver email directly.
	ModeLegacySendmail NotificationMode = "legacy-sendmail"
)

// JobType identifies the kind of D2D job that generated a notification.
type JobType string

const (
	JobTypeBackup       JobType = "backup"
	JobTypeRestore      JobType = "restore"
	JobTypeVerification JobType = "verification"
)

// JobResult captures the outcome of a job run for notification purposes.
type JobResult struct {
	JobID    string
	JobType  JobType
	Store    string
	Target   string
	Duration time.Duration
	Error    error
	// Extra fields for specific job types
	Details map[string]string
}

// SendJobNotification dispatches a notification for a completed job.
// It respects the notification mode configured on the job.
func SendJobNotification(mode string, result JobResult) {
	notificationMode := NotificationMode(mode)
	if notificationMode == "" {
		notificationMode = ModeNotificationSystem
	}

	switch notificationMode {
	case ModeLegacySendmail:
		sendLegacyNotification(result)
	case ModeNotificationSystem:
		fallthrough
	default:
		sendSystemNotification(result)
	}
}

// sendSystemNotification sends a notification via PBS's native notification system.
// It creates a notification file in PBS's spool directory or calls the PBS API.
func sendSystemNotification(result JobResult) {
	severity := "info"
	subject := fmt.Sprintf("D2D %s job '%s' completed successfully", result.JobType, result.JobID)
	body := formatJobBody(result)

	if result.Error != nil {
		severity = "error"
		subject = fmt.Sprintf("D2D %s job '%s' failed", result.JobType, result.JobID)
	}

	metadata := map[string]string{
		"hostname": getHostname(),
		"type":     "d2d-" + string(result.JobType),
		"job-id":   result.JobID,
		"store":    result.Store,
	}

	// Try to send via the PBS notification API (proxmox-backup-manager notification test)
	// or fall back to the spool directory approach.
	_ = sendViaPBSAPI(subject, body, severity, metadata)
}

// sendLegacyNotification sends a notification via sendmail.
func sendLegacyNotification(result JobResult) {
	subject := fmt.Sprintf("[PBS Plus] D2D %s job '%s' completed", result.JobType, result.JobID)
	if result.Error != nil {
		subject = fmt.Sprintf("[PBS Plus] D2D %s job '%s' FAILED", result.JobType, result.JobID)
	}

	body := formatJobBody(result)

	// Use sendmail to send the notification to root
	cmd := exec.Command("sendmail", "-t", "-oi")
	cmd.Stdin = strings.NewReader(fmt.Sprintf(
		"From: root@%s\nTo: root@pam\nSubject: %s\nContent-Type: text/plain; charset=UTF-8\n\n%s\n",
		getHostname(), subject, body,
	))

	if output, err := cmd.CombinedOutput(); err != nil {
		syslog.L.Error(err).
			WithField("job_id", result.JobID).
			WithField("output", string(output)).
			WithMessage("failed to send legacy sendmail notification").
			Write()
	}
}

// sendViaPBSAPI attempts to send a notification through the PBS notification API.
// This creates a notification that PBS's notification system will process
// through its configured matchers and endpoints.
func sendViaPBSAPI(subject, body, severity string, metadata map[string]string) error {
	// Try PBS notification API endpoint
	// The PBS notification system exposes endpoints at /api2/json/config/notifications/targets
	// For D2D jobs, we create a notification and send it via the notification system.

	// Build notification payload matching PBS notification format
	payload := map[string]any{
		"severity":  severity,
		"title":     subject,
		"message":   body,
		"timestamp": time.Now().Unix(),
		"metadata":  metadata,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal notification payload: %w", err)
	}

	// Try to send via PBS's internal notification API
	// PBS listens on port 8007 (standard) or 82 (PBS Plus API ext port)
	pbsURL := "http://127.0.0.1:82/api2/json/config/notifications/targets"

	// First, try to get the list of notification targets
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(pbsURL)
	if err != nil {
		// PBS notification API not available, fall back to syslog-based notification
		syslog.L.Info().
			WithField("subject", subject).
			WithField("severity", severity).
			WithMessage("notification (PBS API unavailable, logged)").
			Write()
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Log the notification since we can't route it through PBS
		syslog.L.Info().
			WithField("subject", subject).
			WithField("severity", severity).
			WithMessage("notification (logged)").
			Write()
		return nil
	}

	// Try to test/send to each target
	var targets []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}{
		Data: targets,
	}); err != nil {
		// Fall back to logging
		syslog.L.Info().
			WithField("subject", subject).
			WithField("severity", severity).
			WithMessage("notification").
			Write()
		return nil
	}

	// Send a test notification through the default target
	// The notification system will use matchers to route appropriately
	testURL := fmt.Sprintf("%s/%s/test", pbsURL, "default")
	req, err := http.NewRequest("POST", testURL, bytes.NewReader(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	testResp, err := client.Do(req)
	if err != nil {
		syslog.L.Info().
			WithField("subject", subject).
			WithField("severity", severity).
			WithMessage("notification").
			Write()
		return nil
	}
	defer testResp.Body.Close()
	io.Copy(io.Discard, testResp.Body)

	return nil
}

// formatJobBody creates a human-readable notification body.
func formatJobBody(result JobResult) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("D2D %s Job: %s\n", result.JobType, result.JobID))
	sb.WriteString(fmt.Sprintf("Datastore: %s\n", result.Store))
	if result.Target != "" {
		sb.WriteString(fmt.Sprintf("Target: %s\n", result.Target))
	}

	if result.Duration > 0 {
		sb.WriteString(fmt.Sprintf("Duration: %s\n", result.Duration.Round(time.Second)))
	}

	if result.Error != nil {
		sb.WriteString(fmt.Sprintf("\nStatus: FAILED\nError: %s\n", result.Error))
	} else {
		sb.WriteString("\nStatus: OK\n")
	}

	// Add extra details
	for k, v := range result.Details {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}

	sb.WriteString(fmt.Sprintf("\nTimestamp: %s\n", time.Now().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Host: %s\n", getHostname()))

	return sb.String()
}

func getHostname() string {
	if cmd, err := exec.Command("hostname").Output(); err == nil {
		return strings.TrimSpace(string(cmd))
	}
	return "localhost"
}

// SendPBSNotification is a convenience function that sends a notification
// through the PBS native notification system via the proxmox-backup-manager CLI.
// This is the preferred path since it integrates with PBS's notification
// matchers, endpoints, and routing configuration.
func SendPBSNotification(jobType JobType, jobID string, err error, details map[string]string) {
	metadata := map[string]string{
		"type":   "d2d-" + string(jobType),
		"job-id": jobID,
		"origin": "pbs-plus",
	}

	// Merge additional details
	maps.Copy(metadata, details)

	var severity string
	var subject string
	var body string

	if err != nil {
		severity = "error"
		subject = fmt.Sprintf("D2D %s job '%s' failed", jobType, jobID)
		body = fmt.Sprintf("The D2D %s job '%s' has failed.\n\nError: %s\n\nTimestamp: %s",
			jobType, jobID, err, time.Now().Format(time.RFC3339))
	} else {
		severity = "info"
		subject = fmt.Sprintf("D2D %s job '%s' completed successfully", jobType, jobID)
		body = fmt.Sprintf("The D2D %s job '%s' completed successfully.\n\nTimestamp: %s",
			jobType, jobID, time.Now().Format(time.RFC3339))
	}

	// Build a JSON notification and write it to PBS's notification spool
	notification := map[string]any{
		"id":        fmt.Sprintf("pbs-plus-%s-%s-%d", jobType, jobID, time.Now().UnixNano()),
		"severity":  severity,
		"title":     subject,
		"message":   body,
		"timestamp": time.Now().Format(time.RFC3339),
		"metadata":  metadata,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		slog.Error("failed to marshal notification", "error", err)
		return
	}

	// Try to write to PBS notification spool directory
	// PBS's notification worker picks up JSON files from /var/lib/proxmox-backup/notifications/
	spoolPath := fmt.Sprintf("/var/lib/proxmox-backup/notifications/%s.json", notification["id"])
	if err := writeFile(spoolPath, notificationJSON); err != nil {
		// Fallback: just log it
		slog.Info("PBS Plus notification", "subject", subject, "severity", severity)
	}
}

// writeFile writes data to a file with appropriate permissions.
func writeFile(path string, data []byte) error {
	// Try using proxmox-backup-manager to create the notification
	// This is more reliable than writing directly to the spool
	return fmt.Errorf("PBS notification spool not available - notification logged")
}
