//go:build linux

package arpcfs

import (
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func (f *ARPCFile) Close() error {
	if f.isClosed.Load() {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return os.ErrInvalid
	}

	req := vssfs.CloseReq{HandleID: f.handleID}
	_, err := f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Close", req)
	if err != nil {
		syslog.L.Errorf("Write RPC failed (%s): %v", f.name, err)
		return err
	}
	f.isClosed.Store(true)

	return nil
}

func (f *ARPCFile) Lseek(off int64, whence int) (uint64, error) {
	req := vssfs.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}
	// Send the request to the server
	respBytes, err := f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Lseek", req)
	if err != nil {
		return 0, os.ErrInvalid
	}

	// Parse the response
	var resp vssfs.LseekResp
	if _, err := resp.UnmarshalMsg(respBytes); err != nil {
		return 0, os.ErrInvalid
	}

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		return 0, os.ErrInvalid
	}

	if f.fs.session == nil {
		return 0, os.ErrInvalid
	}

	req := vssfs.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	bytesRead, err := f.fs.session.CallMsgWithBuffer(f.fs.ctx, f.jobId+"/ReadAt", req, p)
	if err != nil {
		syslog.L.Errorf("Read RPC failed (%s): %v", f.name, err)
		return 0, err
	}

	go atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))

	// If we read less than requested, it indicates EOF
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
