//go:build linux

package targetplugin

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestHostClosedCoversSocketTeardown(t *testing.T) {
	closed := []error{
		nil,
		io.EOF,
		io.ErrUnexpectedEOF,
		io.ErrClosedPipe,
		net.ErrClosed,
		fmt.Errorf("session closed: %w", syscall.ECONNRESET),
		&net.OpError{Op: "read", Net: "unix", Err: syscall.ECONNRESET},
		&net.OpError{Op: "write", Net: "unix", Err: syscall.EPIPE},
	}
	for _, err := range closed {
		if !hostClosed(err) {
			t.Fatalf("hostClosed(%v) = false, want true", err)
		}
	}
	if hostClosed(errors.New("plugin handler failed")) {
		t.Fatal("hostClosed reported a plugin failure as a clean shutdown")
	}
}
