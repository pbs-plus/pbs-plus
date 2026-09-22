//go:build linux

package backup

import (
	"os"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestHandlePluginEventWritesClientLog(t *testing.T) {
	jobID := "plugin-event-" + strings.ReplaceAll(t.Name(), "/", "-")
	logger := log.WithScope(log.Scope{JobID: jobID})
	defer logger.Close()

	job := &backupJob{logger: logger}
	const marker = "--- PostgreSQL log starts here ---"
	if err := job.handlePluginEvent(targetplugin.HostEvent{Level: targetplugin.EventInfo, Message: marker}); err != nil {
		t.Fatalf("handlePluginEvent: %v", err)
	}
	if err := logger.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	content, err := os.ReadFile(logger.JobLogPath())
	if err != nil {
		t.Fatalf("read job log: %v", err)
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		if line == marker {
			return
		}
	}
	t.Fatalf("job log does not contain raw plugin event:\n%s", content)
}
