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
	sendAlertWithData(alertType, severity, details, nil)
}

// SendAlertWithData is like SendAlert but also accepts structured data for templates.
// The extraData map is merged at the top level of template data, allowing arrays/objects.
func SendAlertWithData(alertType AlertType, severity string, details map[string]string, extraData map[string]any) {
	sendAlertWithData(alertType, severity, details, extraData)
}

func sendAlertWithData(alertType AlertType, severity string, details map[string]string, extraData map[string]any) {
	ts := time.Now()

	fields := map[string]string{
		"hostname": getHostname(),
		"type":     "d2d-alert-" + string(alertType),
	}
	maps.Copy(fields, details)

	templateName := "d2d-alert-" + string(alertType)
	title := formatAlertTitle(alertType, details)

	tmplData := func() map[string]any {
		m := map[string]any{
			"timestamp": ts.Format(time.RFC3339),
			"title":     title,
		}
		// Flatten details into top level so templates can use {{target-name}}, {{job-id}}, etc.
		for k, v := range details {
			if _, exists := m[k]; !exists {
				m[k] = v
			}
		}
		maps.Copy(m, extraData)
		return m
	}()
	tmplJSON, _ := json.Marshal(tmplData)

	tc := templateContent{
		TemplateName: templateName,
		Data:         tmplJSON,
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
		count := details["count"]
		if count == "" {
			count = "unknown"
		}
		return fmt.Sprintf("%s unconfigured target(s) detected", count)
	case AlertStaleBackup:
		count := details["count"]
		if count == "" {
			count = "unknown"
		}
		threshold := details["threshold"]
		if threshold == "" {
			threshold = "unknown"
		}
		return fmt.Sprintf("%s stale backup job(s) detected (threshold: %s days)", count, threshold)
	case AlertTargetOffline:
		name := details["target-name"]
		return fmt.Sprintf("Target offline: %s", name)
	default:
		return fmt.Sprintf("D2D alert: %s", string(alertType))
	}
}
