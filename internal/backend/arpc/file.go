//go:build linux

package arpcfs

import (
	"io"
	"os"
	"sync/atomic"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

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

	req := vssfs.CloseReq{HandleID: f.handleID}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return os.ErrInvalid
	}

	_, err = f.fs.session.CallMsg(ctx, f.jobId+"/Close", reqBytes)
	f.isClosed = true
	if err != nil {
		syslog.L.Errorf("Write RPC failed (%s): %v", f.name, err)
		return err
	}

	return nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed {
		return 0, os.ErrInvalid
	}

	if f.fs.session == nil {
		return 0, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := vssfs.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return 0, os.ErrInvalid
	}

	bytesRead, isEOF, err := f.fs.session.CallMsgWithBuffer(ctx, f.jobId+"/ReadAt", reqBytes, p)
	if err != nil {
		syslog.L.Errorf("Read RPC failed (%s): %v", f.name, err)
		return 0, err
	}

	go atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))

	if isEOF {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
