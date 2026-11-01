//go:build unix

package log

import (
	"fmt"
	"log/slog"
	"log/syslog"
	"os"
	"slices"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/host"
)

type LogWriter struct {
	logger *syslog.Writer
}

func (sw *LogWriter) Write(p []byte) (n int, err error) {
	message := string(p)
	if sw.logger != nil {
		if strings.Contains(message, "ERR") {
			err = sw.logger.Err(message)
		} else {
			err = sw.logger.Info(message)
		}
	} else {
		return os.Stdout.Write(p)
	}
	return len(p), err
}

func setServiceLogger(l *Logger) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	hostname, err := host.AgentHostname()
	if err != nil {
		hostname = "localhost"
	}
	l.hostname = hostname

	tag := "pbs-plus-agent"
	if conf.IsServer {
		tag = "pbs-plus"
	}

	sysWriter, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, tag)
	if err != nil {
		return nil
	}

	handler := slog.NewJSONHandler(&LogWriter{logger: sysWriter}, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: false,
	})
	if conf.Env.Debug {
		handler = slog.NewJSONHandler(&LogWriter{logger: sysWriter}, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: false,
		})
	}

	l.zlog = slog.New(handler)
	l.zlog.Info("service logger successfully added for syslog")
	return nil
}

func (l *Logger) writePlatform(e *entry, attrs []any) {
	if l.Server {
		l.writeServer(e, attrs)
		return
	}
	l.zlog.Log(ctxNone, levelFromEntry(e), e.message, attrs...)
}

var jobLogIdentityFields = []string{"hostname", "jobID", "backupID", "restoreID", "verifyID"}

// formatJobLogLine keeps the "WARNING: " prefix because processPBSProxyLogs counts that marker.
func formatJobLogLine(e *entry) string {
	var sb strings.Builder
	sb.WriteString("pbs-plus: ")
	if e.level == "error" || e.level == "warn" {
		sb.WriteString("WARNING: ")
	}
	if e.message != "" {
		sb.WriteString(e.message)
	}
	if e.err != nil {
		if e.message != "" {
			sb.WriteString(": ")
		}
		sb.WriteString(e.err.Error())
	}

	keys := make([]string, 0, len(e.fields))
	for k := range e.fields {
		if slices.Contains(jobLogIdentityFields, k) {
			continue
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Fprintf(&sb, " %s=%v", k, e.fields[k])
	}
	return sb.String()
}

func (l *Logger) writeServer(e *entry, attrs []any) {
	if e.level == "debug" && os.Getenv("DEBUG") != "true" {
		return
	}

	if e.jobID != "" {
		jl := getExistingJobLogger(e.jobID)
		if jl != nil {
			if _, err := jl.Write([]byte(formatJobLogLine(e))); err != nil {
				slog.Error("failed to write to job logger", "error", err)
			}
		}
	}

	l.zlog.Log(ctxNone, levelFromEntry(e), e.message, attrs...)
}
