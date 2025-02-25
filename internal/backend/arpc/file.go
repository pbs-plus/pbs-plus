//go:build linux

package arpcfs

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

// File implementation
func (f *ARPCFile) Name() string {
	return f.name
}

func (f *ARPCFile) Read(p []byte) (int, error) {
	if f.isClosed {
		return 0, os.ErrClosed
	}

	var resp ReadResponse
	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return 0, fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := f.fs.session.CallJSON(ctx, f.drive+"/Read", ReadRequest{
		HandleID: f.handleID,
		Length:   len(p),
	}, &resp)
	if err != nil {
		syslog.L.Errorf("Read RPC failed (%s): %v", f.name, err)
		return 0, fmt.Errorf("Read RPC failed: %w", err)
	}

	copy(p, resp.Data)
	f.offset += int64(len(resp.Data))

	if resp.EOF {
		return len(resp.Data), io.EOF
	}
	return len(resp.Data), nil
}

func (f *ARPCFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read-only filesystem")
}

func (f *ARPCFile) Close() error {
	if f.isClosed {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	_, err := f.fs.session.CallContext(ctx, f.drive+"/Close", struct {
		HandleID string `json:"handleID"`
	}{
		HandleID: f.handleID,
	})
	f.isClosed = true
	return err
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed {
		return 0, os.ErrClosed
	}

	var resp ReadResponse
	if f.fs.session == nil {
		return 0, fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := f.fs.session.CallJSON(ctx, f.drive+"/ReadAt", ReadRequest{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}, &resp)
	if err != nil {
		syslog.L.Errorf("ReadAt RPC failed (%s): %v", f.name, err)
		return 0, fmt.Errorf("ReadAt RPC failed: %w", err)
	}

	copy(p, resp.Data)

	if resp.EOF {
		return len(resp.Data), io.EOF
	}
	return len(resp.Data), nil
}

func (f *ARPCFile) Seek(offset int64, whence int) (int64, error) {
	if f.isClosed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		var fi FileInfoResponse
		if f.fs.session == nil {
			syslog.L.Error("RPC failed: aRPC session is nil")
			return 0, fmt.Errorf("RPC failed: aRPC session is nil")
		}

		ctx, cancel := TimeoutCtx()
		defer cancel()

		err := f.fs.session.CallJSON(ctx, f.drive+"/Fstat", struct {
			HandleID string `json:"handleID"`
		}{
			HandleID: f.handleID,
		}, &fi)
		if err != nil {
			syslog.L.Errorf("Fstat RPC failed (%s): %v", f.name, err)
			return 0, fmt.Errorf("Fstat RPC failed: %w", err)
		}
		f.offset = fi.Size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	return f.offset, nil
}

func (f *ARPCFile) Lock() error {
	return fmt.Errorf("locking not supported")
}

func (f *ARPCFile) Unlock() error {
	return fmt.Errorf("locking not supported")
}

func (f *ARPCFile) Truncate(size int64) error {
	return fmt.Errorf("read-only filesystem")
}

// fileInfo implements os.FileInfo
type fileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() interface{}   { return nil }
