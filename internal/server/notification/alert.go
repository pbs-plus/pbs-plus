package notification

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// AlertType identifies a system-level D2D alert.
type AlertType string

const (
	AlertUnconfiguredTarget AlertType = "unconfigured-target"
	AlertStaleBackup        AlertType = "stale-backup"
	AlertTargetOffline      AlertType = "target-offline"
)

// SendAlert dispatches a system-level alert notification via the PBS spool.
// Unlike Send (which is for job completion), alerts are for monitoring conditions.
func SendAlert(alertType AlertType, severity string, details map[string]string) {
	ts := time.Now()

	fields := map[string]string{
		"hostname": getHostname(),
		"type":     "d2d-alert-" + string(alertType),
	}
	maps.Copy(fields, details)

	templateName := "d2d-alert-" + string(alertType)
	title := formatAlertTitle(alertType, details)

	tmplData, _ := json.Marshal(map[string]any{
		"timestamp": ts.Format(time.RFC3339),
		"title":     title,
		"details":   details,
	})

	tc := templateContent{
		TemplateName: templateName,
		Data:         tmplData,
	}
	tcJSON, err := json.Marshal(tc)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to marshal alert template content").Write()
		return
	}

	wrappedContent, err := json.Marshal(map[string]json.RawMessage{
		"template": tcJSON,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to wrap alert template content").Write()
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

	sendViaSpool(n)
}

func formatAlertTitle(alertType AlertType, details map[string]string) string {
	switch alertType {
	case AlertUnconfiguredTarget:
		name := details["target-name"]
		if name == "" {
			name = "unknown"
		}
		return fmt.Sprintf("Unconfigured target detected: %s", name)
	case AlertStaleBackup:
		jobID := details["job-id"]
		days := details["days-stale"]
		return fmt.Sprintf("Stale backup: job '%s' has not run in %s days", jobID, days)
	case AlertTargetOffline:
		name := details["target-name"]
		return fmt.Sprintf("Target offline: %s", name)
	default:
		return fmt.Sprintf("D2D alert: %s", string(alertType))
	}
}
