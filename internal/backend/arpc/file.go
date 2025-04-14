//go:build linux

package arpcfs

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (f *ARPCFile) Close() error {
	if f.isClosed.Load() {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithJob(f.jobId).WithMessage("arpc session is nil").Write()
		return syscall.ENOENT
	}

	req := types.CloseReq{HandleID: f.handleID}
	_, err := f.fs.session.CallMsgWithTimeout(1*time.Minute, f.jobId+"/Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		syslog.L.Error(err).WithJob(f.jobId).WithMessage("failed to handle close request").WithField("path", f.name).Write()
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
	respBytes, err := f.fs.session.CallMsgWithTimeout(1*time.Minute, f.jobId+"/Lseek", &req)
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).WithMessage("lseek call failed").WithField("path", f.name).Write()
		return 0, syscall.ENOENT
	}

	// Parse the response
	var resp types.LseekResp
	if err := resp.Decode(respBytes); err != nil {
		syslog.L.Error(err).WithJob(f.jobId).WithMessage("failed to handle lseek request").WithField("path", f.name).Write()
		return 0, syscall.ENOENT
	}

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		syslog.L.Error(syscall.ENOENT).WithJob(f.jobId).WithMessage("file is closed").WithField("path", f.name).Write()
		return 0, syscall.ENOENT
	}

	if f.fs.session == nil {
		syslog.L.Error(syscall.ENOENT).WithJob(f.jobId).WithMessage("fs session is nil").WithField("path", f.name).Write()
		return 0, syscall.ENOENT
	}

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	buf, bytesRead, err := f.fs.session.CallBinary(f.fs.ctx, f.jobId+"/ReadAt", &req)
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("failed to handle read request, replace failed reads with zeroes, likely corrupted").
			WithField("path", f.name).
			WithField("offset", f.offset).
			WithField("length", len(p)).
			Write()

		return 0, io.EOF
	}
	copy(p, buf)

	atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))

	// If we read less than requested, it indicates EOF
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
