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
	if f.isClosed {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return os.ErrInvalid
	}

	req := vssfs.CloseReq{HandleID: f.handleID}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return os.ErrInvalid
	}

	_, err = f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Close", reqBytes)
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

	req := vssfs.ReadAtReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Length:   len(p),
	}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return 0, os.ErrInvalid
	}

	totalBytesRead := 0
	for totalBytesRead < len(p) {
		bytesRead, err := f.fs.session.CallMsgWithBuffer(f.fs.ctx, f.jobId+"/ReadAt", reqBytes, p[totalBytesRead:])
		totalBytesRead += bytesRead

		go atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))
		if err != nil {
			if err == io.EOF && totalBytesRead > 0 {
				// Partial read, return bytesRead without EOF
				return totalBytesRead, nil
			}
			return totalBytesRead, err
		}

		if bytesRead == 0 {
			break
		}
	}

	// Return EOF only if we're at the end of the file
	if totalBytesRead == 0 {
		return 0, io.EOF
	}

	return totalBytesRead, nil
}
