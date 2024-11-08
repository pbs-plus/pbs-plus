package serverlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent"
)

type Logger struct {
	Hostname string
}

func InitializeLogger() (*Logger, error) {
	hostname, _ := os.Hostname()
	return &Logger{Hostname: hostname}, nil
}

type LogRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
}

func (l *Logger) Print(v string) {
	body, err := json.Marshal(&LogRequest{
		Hostname: l.Hostname,
		Message:  v,
	})
	if err != nil {
		log.Println(fmt.Errorf("Print: error marshalling request body -> %w", err).Error())
	}

	err = agent.ProxmoxHTTPRequest(http.MethodPost, "/ap2/json/d2d/agent-log", bytes.NewBuffer(body), nil)
	if err != nil {
		log.Println(fmt.Errorf("Print: error posting log to server -> %w", err).Error())
	}
}
