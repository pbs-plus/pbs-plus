package arpc

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/xtaci/smux"
)

func HijackUpgradeHTTP(w http.ResponseWriter, r *http.Request, hostname string, version string, mgr *SessionManager, config *smux.Config) (*Session, error) {
	if w == nil {
		return nil, fmt.Errorf("response writer is nil")
	}
	if r == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if mgr == nil {
		return nil, fmt.Errorf("session manager is nil")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("response writer does not support hijacking")
	}

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	if conn == nil {
		return nil, fmt.Errorf("hijacked connection is nil")
	}

	if rw == nil {
		conn.Close()
		return nil, fmt.Errorf("hijacked readwriter is nil")
	}

	_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n\r\n")
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	session, err := mgr.GetOrCreateSession(hostname, version, conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return session, nil
}

func upgradeHTTPClient(conn net.Conn, requestPath, host string, headers http.Header, config *smux.Config) (*Session, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	if requestPath == "" {
		requestPath = "/"
	}
	if host == "" {
		return nil, fmt.Errorf("host is required")
	}

	reqLines := []string{
		fmt.Sprintf("GET %s HTTP/1.1", requestPath),
		fmt.Sprintf("Host: %s", host),
	}
	if headers != nil {
		for key, values := range headers {
			if key == "" {
				continue
			}
			for _, value := range values {
				reqLines = append(reqLines, fmt.Sprintf("%s: %s", key, value))
			}
		}
	}
	reqLines = append(reqLines,
		"Upgrade: tcp",
		"Connection: Upgrade",
		"", "",
	)
	reqStr := strings.Join(reqLines, "\r\n")

	if _, err := conn.Write([]byte(reqStr)); err != nil {
		return nil, fmt.Errorf("failed to write upgrade request: %w", err)
	}

	reader := bufio.NewReader(conn)
	if reader == nil {
		return nil, fmt.Errorf("failed to create reader")
	}

	statusLine, _, err := reader.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("failed to read status line: %w", err)
	}

	statusStr := string(statusLine)
	if !strings.Contains(statusStr, "101") {
		return nil, fmt.Errorf("expected status 101, got: %s", statusStr)
	}

	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("failed to read header line: %w", err)
		}
		if isPrefix {
			return nil, fmt.Errorf("header line too long")
		}
		if len(line) == 0 {
			break
		}
	}

	session, err := NewClientSession(conn, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client session: %w", err)
	}

	return session, nil
}
