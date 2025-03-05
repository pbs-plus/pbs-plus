//go:build linux

package arpcfs

import (
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
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

	req := types.CloseReq{HandleID: f.handleID}
	_, err := f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Close", &req)
	if err != nil && err != os.ErrNotExist {
		syslog.L.Errorf("Close RPC failed (%s): %v", f.name, err)
		return err
	}
	f.isClosed.Store(true)

	return nil
}

func (f *ARPCFile) Lseek(off int64, whence int) (uint64, error) {
	req := types.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}
	// Send the request to the server
	respBytes, err := f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Lseek", &req)
	if err != nil {
		return 0, err
	}

	// Parse the response
	var resp types.LseekResp
	if err := resp.Decode(respBytes); err != nil {
		syslog.L.Errorf("Lseek RPC failed (%s): %v", f.name, err)
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

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	bytesRead, err := f.fs.session.CallBinary(f.fs.ctx, f.jobId+"/ReadAt", &req, p)
	if err != nil {
		syslog.L.Errorf("Read RPC failed (%s): %v", f.name, err)
		return 0, err
	}

	atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))

	// If we read less than requested, it indicates EOF
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
