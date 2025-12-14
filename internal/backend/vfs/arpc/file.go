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
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/quic-go/quic-go"
)

func (f *ARPCFile) Close() error {
	if f.isClosed.Load() {
		syslog.L.Debug().
			WithMessage("Close called on already closed file").
			WithJob(f.jobId).
			WithField("path", f.name).
			Write()
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithJob(f.jobId).
			WithMessage("arpc session is nil").
			WithField("path", f.name).
			Write()
		return syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("Issuing Close RPC").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	req := types.CloseReq{HandleID: f.handleID}
	_, err := f.fs.session.CallDataWithTimeout(1*time.Minute, f.jobId+"/Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		syslog.L.Error(err).
			WithJob(f.jobId).
			WithMessage("failed to handle close request").
			WithField("path", f.name).
			WithField("handleID", f.handleID).
			Write()
		return err
	}
	f.isClosed.Store(true)

	syslog.L.Debug().
		WithMessage("Close RPC completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	return nil
}

func (f *ARPCFile) Lseek(off int64, whence int) (uint64, error) {
	syslog.L.Debug().
		WithMessage("Lseek called").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("whence", whence).
		Write()

	req := types.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}
	respBytes, err := f.fs.session.CallDataWithTimeout(1*time.Minute, f.jobId+"/Lseek", &req)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.jobId).
			WithMessage("lseek call failed").
			WithField("path", f.name).
			WithField("offset", off).
			WithField("whence", whence).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	var resp types.LseekResp
	if err := resp.Decode(respBytes); err != nil {
		syslog.L.Error(err).
			WithJob(f.jobId).
			WithMessage("failed to handle lseek request").
			WithField("path", f.name).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	syslog.L.Debug().
		WithMessage("Lseek completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("newOffset", resp.NewOffset).
		Write()

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		syslog.L.Error(syscall.ENOENT).
			WithJob(f.jobId).
			WithMessage("file is closed").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	if f.fs.session == nil {
		syslog.L.Error(syscall.ENOENT).
			WithJob(f.jobId).
			WithMessage("fs session is nil").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("ReadAt called").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("length", len(p)).
		Write()

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	bytesRead := 0
	err := f.fs.session.Call(f.fs.Ctx, f.jobId+"/ReadAt", &req, arpc.RawStreamHandler(func(s *quic.Stream) error {
		n, err := binarystream.ReceiveDataInto(s, p)
		if err != nil {
			return err
		}
		bytesRead = n

		return nil
	}))
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("failed to handle read request, replace failed reads with zeroes, likely corrupted").
			WithField("path", f.name).
			WithField("offset", f.offset).
			WithField("length", len(p)).
			Write()

		return 0, io.EOF
	}

	atomic.AddInt64(&f.fs.TotalBytes, int64(bytesRead))

	syslog.L.Debug().
		WithMessage("ReadAt completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("requested", len(p)).
		WithField("bytesRead", bytesRead).
		Write()

	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
