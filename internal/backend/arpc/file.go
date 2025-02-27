//go:build linux

package arpcfs

import (
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
		return 0, os.ErrInvalid
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return 0, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	// Use the new direct buffer method
	bytesRead, isEOF, err := f.fs.session.CallMsgWithBuffer(ctx, f.jobId+"/Read", ReadRequest{
		HandleID: f.handleID,
		Length:   len(p),
	}, p)

	if err != nil {
		syslog.L.Errorf("Read RPC failed (%s): %v", f.name, err)
		return 0, os.ErrInvalid
	}

	f.offset += int64(bytesRead)

	f.fs.totalBytesMu.Lock()
	f.fs.totalBytes += uint64(bytesRead)
	f.fs.totalBytesMu.Unlock()

	if isEOF {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}

func (f *ARPCFile) Write(p []byte) (n int, err error) {
	return 0, os.ErrInvalid
}

func (f *ARPCFile) Close() error {
	if f.isClosed {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	_, err := f.fs.session.CallContext(ctx, f.jobId+"/Close", struct {
		HandleID uint64 `json:"handleID"`
	}{
		HandleID: f.handleID,
	})
	f.isClosed = true
	if err != nil {
		syslog.L.Errorf("Write RPC failed (%s): %v", f.name, err)
		return os.ErrInvalid
	}

	return nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed {
		return 0, os.ErrInvalid
	}

	var resp ReadResponse
	if f.fs.session == nil {
		return 0, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := f.fs.session.CallMsg(ctx, f.jobId+"/ReadAt", ReadRequest{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}, &resp)
	if err != nil {
		syslog.L.Errorf("ReadAt RPC failed (%s): %v", f.name, err)
		return 0, os.ErrInvalid
	}

	copy(p, resp.Data)

	if resp.EOF {
		return len(resp.Data), io.EOF
	}
	return len(resp.Data), nil
}

func (f *ARPCFile) Seek(offset int64, whence int) (int64, error) {
	if f.isClosed {
		return 0, os.ErrInvalid
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
			return 0, os.ErrInvalid
		}

		ctx, cancel := TimeoutCtx()
		defer cancel()

		err := f.fs.session.CallMsg(ctx, f.jobId+"/Fstat", struct {
			HandleID uint64 `json:"handleID"`
		}{
			HandleID: f.handleID,
		}, &fi)
		if err != nil {
			syslog.L.Errorf("Fstat RPC failed (%s): %v", f.name, err)
			return 0, os.ErrInvalid
		}
		f.offset = fi.Size + offset
	default:
		return 0, os.ErrInvalid
	}
	return f.offset, nil
}

func (f *ARPCFile) Lock() error {
	return os.ErrInvalid
}

func (f *ARPCFile) Unlock() error {
	return os.ErrInvalid
}

func (f *ARPCFile) Truncate(size int64) error {
	return os.ErrInvalid
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
