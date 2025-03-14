//go:build linux

package arpcfs

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func (f *ARPCFile) Close() error {
	if f.isClosed.Load() {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return syscall.EIO
	}

	req := types.CloseReq{HandleID: f.handleID}
	_, err := f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		syslog.L.Error(err).WithMessage("failed to handle close request").WithField("name", f.name).Write()
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
		if arpc.IsOSError(err) {
			return 0, err
		}
		return 0, syscall.EIO
	}

	// Parse the response
	var resp types.LseekResp
	if err := resp.Decode(respBytes); err != nil {
		syslog.L.Error(err).WithMessage("failed to handle lseek request").WithField("name", f.name).Write()
		return 0, syscall.EIO
	}

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		return 0, syscall.EIO
	}

	if f.fs.session == nil {
		return 0, syscall.EIO
	}

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	bytesRead, err := f.fs.session.CallBinary(f.fs.ctx, f.jobId+"/ReadAt", &req, p)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to handle read request").WithField("name", f.name).Write()
		if arpc.IsOSError(err) {
			return 0, err
		}
		return 0, syscall.EIO
	}

	atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))

	// If we read less than requested, it indicates EOF
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
