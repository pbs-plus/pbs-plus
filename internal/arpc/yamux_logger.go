package arpc

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type YamuxLoggerAdapter struct {
	Underlying *syslog.Logger
}

func (l *YamuxLoggerAdapter) Print(v ...interface{}) {
	l.Underlying.Info().WithMessage(fmt.Sprint(v...))
}

func (l *YamuxLoggerAdapter) Printf(format string, v ...interface{}) {
	l.Underlying.Info().WithMessage(fmt.Sprintf(format, v...))
}

func (l *YamuxLoggerAdapter) Println(v ...interface{}) {
	l.Underlying.Info().WithMessage(fmt.Sprintln(v...))
}
