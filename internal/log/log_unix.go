//go:build unix

package log

import (
	"fmt"
	"log/slog"
	"log/syslog"
	"os"
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

func (l *Logger) writeServer(e *entry, attrs []any) {
	if e.level == "debug" && os.Getenv("DEBUG") != "true" {
		return
	}

	if e.jobID != "" {
		jl := getExistingJobLogger(e.jobID)
		if jl != nil {
			var sb strings.Builder
			sb.WriteString("pbs-plus: ")
			if e.level == "error" {
				sb.WriteString("WARNING: [non-fatal " + e.level + "]")
			} else {
				sb.WriteString("[" + e.level + "]")
			}
			if e.err != nil {
				sb.WriteString(" " + e.err.Error())
			}
			if e.message != "" {
				sb.WriteString(": " + e.message)
			}
			if len(e.fields) > 0 {
				sb.WriteString(fmt.Sprintf(" (debug values: %v)", e.fields))
			}
			if _, err := jl.Write([]byte(sb.String())); err != nil {
				slog.Error("failed to write to job logger", "error", err)
			}
		}
	}

	l.zlog.Log(ctxNone, levelFromEntry(e), e.message, attrs...)
}
