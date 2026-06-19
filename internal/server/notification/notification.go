//go:build linux

package notification

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	// SpoolDir is the PBS notification spool directory.
	// PBS's notification_worker (running as root) reads JSON files from here
	// every 5 seconds and routes them through matchers/endpoints.
	SpoolDir = "/var/lib/proxmox-backup/notifications"

	// DefaultMode is used when no notification-mode is set on a job.
	DefaultMode = "notification-system"

	// VendorTemplateDir is where PBS ships its built-in templates.
	// Our custom templates must be installed here (or in the override dir).
	VendorTemplateDir = "/usr/share/proxmox-backup/templates/default"

	// OverrideTemplateDir is where user-provided template overrides live.
	OverrideTemplateDir = "/etc/proxmox-backup/notification-templates/default"
)

// NotificationMode determines how job notifications are delivered.
type NotificationMode string

const (
	ModeNotificationSystem NotificationMode = "notification-system"
	ModeLegacySendmail     NotificationMode = "legacy-sendmail"
)

// JobType identifies the kind of D2D job.
type JobType string

const (
	JobTypeBackup       JobType = "backup"
	JobTypeRestore      JobType = "restore"
	JobTypeVerification JobType = "verification"
)

// notification is the JSON-serializable representation of a PBS Notification.
// When written to the spool directory, PBS's notification_worker picks it up
// and routes it through configured matchers and endpoints.
//
// This struct mirrors proxmox_notify::Notification from the proxmox-notify crate.
// All JSON field names use kebab-case to match the Rust serde rename_all = "kebab-case".
// The content field uses an externally-tagged enum representation matching
// serde's default enum serialization for Content::Template { ... }.
type notification struct {
	// Content is the externally-tagged enum: {"template": {"template-name": "...", "data": {...}}}
	Content  json.RawMessage `json:"content"`
	Metadata metadata        `json:"metadata"`
	ID       string          `json:"id"`
}

// templateContent is the inner payload for Content::Template.
// Serialized under the "template" key to match Rust's externally-tagged enum.
type templateContent struct {
	TemplateName string          `json:"template-name"`
	Data         json.RawMessage `json:"data"`
}

type metadata struct {
	Severity         string            `json:"severity"`
	Timestamp        int64             `json:"timestamp"`
	AdditionalFields map[string]string `json:"additional-fields"`
}

// Send dispatches a notification for a completed D2D job.
//
// For ModeNotificationSystem (default), it writes a JSON file to PBS's
// notification spool directory (/var/lib/proxmox-backup/notifications/).
// The PBS notification worker (running as root) reads this directory every 5s,
// loads the notification config (matchers, endpoints), and routes the
// notification through all matching endpoints  -  exactly the same mechanism
// PBS uses internally for non-root processes.
//
// The additional-fields map includes type, job-id, datastore, and hostname,
// which PBS notification matchers can filter on. The "type" field uses values
// like "d2d-backup", "d2d-restore", "d2d-verify"  -  these appear in the
// PBS matcher UI dropdown alongside the built-in types (gc, sync, verify, etc.).
//
// For ModeLegacySendmail, it invokes /usr/sbin/sendmail directly.
func Send(mode string, jobType JobType, jobID, datastore string, jobErr error, details map[string]string) {
	nm := NotificationMode(mode)
	if nm == "" {
		nm = ModeNotificationSystem
	}

	// Map job result to PBS severity levels.
	// See proxmox-notify Severity enum: info, notice, warning, error, unknown.
	severity := "info"
	if jobErr != nil {
		severity = "error"
	}
	// If details indicate warnings but the job didn't hard-fail, upgrade to notice.
	if severity == "info" && details != nil {
		if warningsStr, ok := details["warnings"]; ok {
			if n, _ := strconv.Atoi(warningsStr); n > 0 {
				severity = "notice"
			}
		}
	}

	ts := time.Now()

	fields := map[string]string{
		"hostname":  getHostname(),
		"type":      "d2d-" + string(jobType),
		"job-id":    jobID,
		"datastore": datastore,
	}
	maps.Copy(fields, details)

	templateName := fmt.Sprintf("d2d-%s-ok", jobType)
	title := fmt.Sprintf("D2D %s job '%s' completed successfully", jobType, jobID)
	if jobErr != nil {
		templateName = fmt.Sprintf("d2d-%s-err", jobType)
		title = fmt.Sprintf("D2D %s job '%s' failed", jobType, jobID)
	}

	tmplData := map[string]any{
		"timestamp": ts.Format(time.RFC3339),
		"title":     title,
		"job-id":    jobID,
		"datastore": datastore,
		"job-type":  string(jobType),
		"error":     errStr(jobErr),
	}
	// Flatten details into top level so templates can use {{total}}, {{target}}, etc.
	for k, v := range details {
		if _, exists := tmplData[k]; !exists {
			tmplData[k] = v
		}
	}
	tmplJSON, _ := json.Marshal(tmplData)

	// Build the externally-tagged Content enum:
	// {"template": {"template-name": "d2d-backup-ok", "data": {...}}}
	tc := templateContent{
		TemplateName: templateName,
		Data:         tmplJSON,
	}
	tcJSON, err := json.Marshal(tc)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to marshal template content").Write()
		return
	}

	wrappedContent, err := json.Marshal(map[string]json.RawMessage{
		"template": tcJSON,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to wrap template content").Write()
		return
	}

	n := notification{
		Content: wrappedContent,
		Metadata: metadata{
			Severity:         severity,
			Timestamp:        ts.Unix(),
			AdditionalFields: fields,
		},
		ID: uuid.New().String(),
	}

	switch nm {
	case ModeLegacySendmail:
		sendLegacy(n, title)
	default:
		sendViaSpool(n)
	}
}

// sendViaSpool writes the notification as JSON to PBS's notification spool.
// This is the exact same mechanism PBS uses for non-root processes:
// the notification_worker task (running as root) picks up JSON files from
// /var/lib/proxmox-backup/notifications/ every 5 seconds and calls
// proxmox_notify::api::common::send(), which evaluates matchers and
// dispatches to configured endpoints (smtp, gotify, webhook, etc.).
func sendViaSpool(n notification) {
	if err := os.MkdirAll(SpoolDir, 0770); err != nil {
		syslog.L.Error(err).WithMessage("failed to create notification spool dir").Write()
		logFallback(n)
		return
	}

	data, err := json.Marshal(n)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to marshal notification").Write()
		logFallback(n)
		return
	}

	path := filepath.Join(SpoolDir, n.ID+".json")
	if err := os.WriteFile(path, data, 0660); err != nil {
		syslog.L.Error(err).
			WithField("path", path).
			WithMessage("failed to write notification spool file").
			Write()
		logFallback(n)
		return
	}

	slog.Info("queued notification via PBS spool", "id", n.ID, "severity", n.Metadata.Severity)
}

// sendLegacy sends a notification directly via sendmail.
// This matches PBS's send_sendmail_legacy_notification behavior:
// it creates a SendmailEndpoint and calls .send() on it, which
// ultimately invokes /usr/sbin/sendmail.
func sendLegacy(n notification, subject string) {
	body := formatBody(n)

	cmd := exec.Command("sendmail", "-t", "-oi")
	cmd.Stdin = strings.NewReader(fmt.Sprintf(
		"From: root@%s\nTo: root@pam\nSubject: %s\nContent-Type: text/plain; charset=UTF-8\n\n%s\n",
		getHostname(), subject, body,
	))

	if output, err := cmd.CombinedOutput(); err != nil {
		syslog.L.Error(err).
			WithField("output", string(output)).
			WithMessage("failed to send legacy sendmail notification").
			Write()
		return
	}

	slog.Info("sent legacy sendmail notification", "id", n.ID)
}

func formatBody(n notification) string {
	var sb strings.Builder
	fields := n.Metadata.AdditionalFields

	sb.WriteString("PBS Plus D2D Notification\n\n")
	fmt.Fprintf(&sb, "Job Type: %s\n", fields["type"])
	fmt.Fprintf(&sb, "Job ID: %s\n", fields["job-id"])
	fmt.Fprintf(&sb, "Datastore: %s\n", fields["datastore"])
	fmt.Fprintf(&sb, "Severity: %s\n", n.Metadata.Severity)
	fmt.Fprintf(&sb, "Host: %s\n", fields["hostname"])
	fmt.Fprintf(&sb, "Time: %s\n", time.Unix(n.Metadata.Timestamp, 0).Format(time.RFC3339))

	if errMsg := fields["error"]; errMsg != "" {
		fmt.Fprintf(&sb, "\nError: %s\n", errMsg)
	}

	return sb.String()
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func getHostname() string {
	if cmd, err := exec.Command("hostname").Output(); err == nil {
		return strings.TrimSpace(string(cmd))
	}
	return "localhost"
}

func logFallback(n notification) {
	title := n.Metadata.AdditionalFields["title"]
	if title == "" {
		title = fmt.Sprintf("notification %s", n.ID)
	}
	slog.Info("PBS Plus notification (spool unavailable)",
		"title", title, "severity", n.Metadata.Severity)
}
