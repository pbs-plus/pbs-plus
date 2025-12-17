//go:build windows

package agentfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

var (
	utf16PathBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]uint16, 512)
			return &buf
		},
	}
	zeroBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 512*1024)
			return &buf
		},
	}
)

func (s *AgentFSServer) abs(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)
	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		syslog.L.Debug().WithMessage("abs: returning snapshot path for root").Write()
		return s.snapshot.Path, nil
	}
	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	syslog.L.Debug().WithMessage("abs: joined path").WithField("path", path).Write()
	return path, nil
}

func toUTF16Z(s string, buf []uint16) []uint16 {
	runes := []rune(s)
	needed := len(runes) + 1
	if cap(buf) < needed {
		buf = make([]uint16, needed)
	} else {
		buf = buf[:needed]
	}
	n := 0
	for _, r := range runes {
		if r < 0x10000 {
			buf[n] = uint16(r)
			n++
		} else {
			r -= 0x10000
			buf[n] = uint16(0xD800 + (r >> 10))
			buf[n+1] = uint16(0xDC00 + (r & 0x3FF))
			n += 2
		}
	}
	buf[n] = 0
	return buf[:n+1]
}

func (s *AgentFSServer) absUNC(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)
	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		unc := "\\\\?\\" + s.snapshot.Path
		syslog.L.Debug().WithMessage("absUNC: returning UNC snapshot path for root").WithField("path", unc).Write()
		return unc, nil
	}
	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	unc := "\\\\?\\" + path
	syslog.L.Debug().WithMessage("absUNC: joined UNC path").WithField("path", unc).Write()
	return unc, nil
}

func (s *AgentFSServer) closeFileHandles() {
	syslog.L.Debug().WithMessage("closeFileHandles: closing all file handles").Write()
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.Lock()
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
			syslog.L.Debug().WithMessage("closeFileHandles: closed mapping handle").WithField("handle_id", u).Write()
			fh.mapping = 0
		}
		if fh.ov != nil {
			_ = fh.ov.Close()
			syslog.L.Debug().WithMessage("closeFileHandles: closed overlapped").WithField("handle_id", u).Write()
			fh.ov = nil
		}
		windows.CloseHandle(fh.handle)
		syslog.L.Debug().WithMessage("closeFileHandles: closed file handle").WithField("handle_id", u).Write()
		fh.Unlock()
		return true
	})
	s.handles.Clear()
	syslog.L.Debug().WithMessage("closeFileHandles: all handles cleared").Write()
}

func (s *AgentFSServer) initializeStatFS() error {
	var err error
	if s.snapshot.SourcePath != "" {
		driveLetter := s.snapshot.SourcePath[:1]
		syslog.L.Debug().WithMessage("initializeStatFS: initializing").WithField("drive", driveLetter).Write()
		s.statFs, err = getStatFS(driveLetter)
		if err != nil {
			syslog.L.Error(err).WithMessage("initializeStatFS: getStatFS failed").WithField("drive", driveLetter).Write()
			return err
		}
		syslog.L.Debug().WithMessage("initializeStatFS: initialized successfully").WithField("drive", driveLetter).Write()
	} else {
		syslog.L.Debug().WithMessage("initializeStatFS: snapshot.SourcePath is empty").Write()
	}
	return nil
}

func (s *AgentFSServer) handleStatFS(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleStatFS: encoding statFs").Write()
	enc, err := cbor.Marshal(s.statFs)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleStatFS: encode failed").Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleStatFS: success").Write()
	return arpc.Response{
		Status: 200,
		Data:   enc,
	}, nil
}

func (s *AgentFSServer) handleOpenFile(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleOpenFile: decoding request").Write()
	var payload types.OpenFileReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		syslog.L.Debug().WithMessage("handleOpenFile: write operation attempted and blocked").WithField("path", payload.Path).Write()
		errStr := "write operations not allowed"
		errBytes, err := cbor.Marshal(errStr)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: encode error message failed").Write()
			return arpc.Response{}, err
		}
		return arpc.Response{
			Status: 403,
			Data:   errBytes,
		}, nil
	}

	path, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	bufPtr := utf16PathBufPool.Get().(*[]uint16)
	pathUTF16 := toUTF16Z(path, *bufPtr)

	desiredAccess := uint32(windows.GENERIC_READ)
	share := uint32(
		windows.FILE_SHARE_READ |
			windows.FILE_SHARE_WRITE |
			windows.FILE_SHARE_DELETE,
	)
	flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS | windows.FILE_FLAG_OVERLAPPED)

	syslog.L.Debug().WithMessage("handleOpenFile: CreateFile").WithField("path", path).Write()
	handle, err := windows.CreateFile(
		&pathUTF16[0],
		desiredAccess,
		share,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	utf16PathBufPool.Put(bufPtr)

	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: CreateFile failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(handle, &std); err != nil {
		windows.CloseHandle(handle)
		syslog.L.Error(err).WithMessage("handleOpenFile: getFileStandardInfoByHandle failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	fh := &FileHandle{
		handle:        handle,
		fileSize:      std.EndOfFile,
		isDir:         std.Directory != 0,
		logicalOffset: 0,
	}
	fh.ov = newOverlapped(handle)

	if fh.isDir {
		dirPath, err := s.abs(payload.Path)
		if err != nil {
			windows.CloseHandle(handle)
			syslog.L.Error(err).WithMessage("handleOpenFile: abs failed for dir").WithField("path", payload.Path).Write()
			return arpc.Response{}, err
		}
		reader, err := NewDirReaderNT(dirPath)
		if err != nil {
			windows.CloseHandle(handle)
			syslog.L.Error(err).WithMessage("handleOpenFile: NewDirReaderNT failed").WithField("path", dirPath).Write()
			return arpc.Response{}, err
		}
		fh.dirReader = reader
	}

	handleId := s.handleIdGen.NextID()
	s.handles.Set(handleId, fh)

	fhId := types.FileHandleId(handleId)
	dataBytes, err := cbor.Marshal(fhId)
	if err != nil {
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
		}
		if fh.ov != nil {
			_ = fh.ov.Close()
		}
		windows.CloseHandle(fh.handle)
		syslog.L.Error(err).WithMessage("handleOpenFile: encode handle id failed").Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleOpenFile: success").WithField("handle_id", handleId).WithField("is_dir", fh.isDir).WithField("size", fh.fileSize).Write()
	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

func (s *AgentFSServer) handleAttr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleAttr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	h, err := openForAttrs(fullPath)
	if err != nil {
		if !strings.HasSuffix(fullPath, ".pxarexclude") {
			syslog.L.Error(err).WithMessage("handleAttr: openForAttrs failed").WithField("path", fullPath).Write()
		}
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var nfo fileNetworkOpenInformation
	if err := ntQueryFileNetworkOpenInformation(h, &nfo); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: ntQueryFileNetworkOpenInformation failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	isDir := (nfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
	size := nfo.EndOfFile

	modTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
	})

	mode := windowsFileModeFromHandle(h, nfo.FileAttributes)
	name := filepath.Base(filepath.Clean(fullPath))

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	var blocks uint64
	if !isDir {
		alloc := nfo.AllocationSize
		if alloc < 0 {
			alloc = 0
		}
		blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.AgentFileInfo{
		Name:    name,
		Size:    size,
		Mode:    mode,
		ModTime: modTime,
		IsDir:   isDir,
		Blocks:  blocks,
	}
	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleAttr: success").WithField("path", fullPath).WithField("is_dir", isDir).WithField("size", size).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleXattr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	h, err := openForAttrs(fullPath)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: openForAttrs failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var creationTime int64
	var lastAccessTime int64
	var lastWriteTime int64
	var fileAttributes map[string]bool

	if !payload.AclOnly {
		var nfo fileNetworkOpenInformation
		if err := ntQueryFileNetworkOpenInformation(h, &nfo); err != nil {
			syslog.L.Error(err).WithMessage("handleXattr: ntQueryFileNetworkOpenInformation failed").WithField("path", fullPath).Write()
			return arpc.Response{}, err
		}

		creationTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.CreationTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.CreationTime) >> 32),
		})
		lastAccessTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.LastAccessTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.LastAccessTime) >> 32),
		})
		lastWriteTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
		})

		fileAttributes = parseFileAttributes(nfo.FileAttributes)
		syslog.L.Debug().WithMessage("handleXattr: file attributes and times acquired").WithField("path", fullPath).Write()
	}
	owner, group, acls, err := GetWinACLsHandle(h)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: GetWinACLsHandle failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	info := types.AgentFileInfo{
		CreationTime:   creationTime,
		LastAccessTime: lastAccessTime,
		LastWriteTime:  lastWriteTime,
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
		WinACLs:        acls,
	}
	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleXattr: success").WithField("path", fullPath).WithField("data", info).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleReadDir(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadDir: decoding request").Write()
	var payload types.ReadDirReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadDir: decode failed").Write()
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleReadDir: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	fh.Lock()
	defer fh.Unlock()

	encodedBatch, err := fh.dirReader.NextBatch(req.Context, s.statFs.Bsize)
	if err != nil {
		if !errors.Is(err, os.ErrProcessDone) {
			syslog.L.Error(err).WithMessage("handleReadDir: error reading batch").WithField("handle_id", payload.HandleID).Write()
		} else {
			syslog.L.Debug().WithMessage("handleReadDir: directory read complete").WithField("handle_id", payload.HandleID).Write()
		}
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()
	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(byteReader, int(len(encodedBatch)), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader via binary stream").WithField("handle_id", payload.HandleID).Write()
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *AgentFSServer) handleReadAt(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadAt: decoding request").Write()
	var payload types.ReadAtReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadAt: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Length < 0 {
		syslog.L.Debug().WithMessage("handleReadAt: negative length requested").WithField("length", payload.Length).Write()
		return arpc.Response{}, fmt.Errorf("invalid negative length requested: %d", payload.Length)
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleReadAt: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		syslog.L.Debug().WithMessage("handleReadAt: attempted read on directory handle").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh.Lock()
	fileSize := fh.fileSize
	handle := fh.handle
	ov := fh.ov
	fh.Unlock()

	if payload.Offset >= fileSize {
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF, sending empty").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("size", fileSize).Write()
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).
						WithMessage("handleReadAt: failed sending empty reader via binary stream").WithField("handle_id", payload.HandleID).Write()
				}
			},
		}, nil
	}

	maxEnd := fileSize
	reqEnd := payload.Offset + int64(payload.Length)
	if reqEnd > maxEnd {
		reqEnd = maxEnd
	}
	if reqEnd <= payload.Offset {
		syslog.L.Debug().WithMessage("handleReadAt: empty range after clamp").WithField("handle_id", payload.HandleID).Write()
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).
						WithMessage("handleReadAt: failed sending empty reader via binary stream").WithField("handle_id", payload.HandleID).Write()
				}
			},
		}, nil
	}
	reqLen := int(reqEnd - payload.Offset)

	ranges, err := queryAllocatedRanges(handle, payload.Offset, int64(reqLen))
	if err != nil || len(ranges) == 0 {
		if err != nil {
			syslog.L.Debug().WithMessage("handleReadAt: queryAllocatedRanges failed, falling back").WithField("handle_id", payload.HandleID).WithField("error", err.Error()).Write()
		} else {
			syslog.L.Debug().WithMessage("handleReadAt: no allocated ranges returned, treating as fully allocated").WithField("handle_id", payload.HandleID).Write()
		}
		ranges = []allocatedRange{{FileOffset: payload.Offset, Length: int64(reqLen)}}
	} else {
		syslog.L.Debug().WithMessage("handleReadAt: using allocated ranges").WithField("handle_id", payload.HandleID).WithField("range_count", len(ranges)).Write()
	}

	streamFn := func(stream *smux.Stream) {
		bufPtr := zeroBufPool.Get().(*[]byte)
		defer zeroBufPool.Put(bufPtr)
		zeroBuf := *bufPtr

		write := func(p []byte) error {
			return binarystream.SendDataFromReader(bytes.NewReader(p), len(p), stream)
		}

		pos := payload.Offset
		end := reqEnd

		for _, r := range ranges {
			if err := req.Context.Err(); err != nil {
				return
			}
			rStart := r.FileOffset
			rEnd := r.FileOffset + r.Length
			if rEnd <= pos {
				continue
			}
			if rStart < pos {
				rStart = pos
			}
			if rEnd > end {
				rEnd = end
			}
			if rStart >= rEnd {
				continue
			}

			if rStart > pos {
				gap := rStart - pos
				for gap > 0 {
					ch := int64(len(zeroBuf))
					if gap < ch {
						ch = gap
					}
					if err := write(zeroBuf[:ch]); err != nil {
						syslog.L.Error(err).WithMessage("handleReadAt: stream write gap failed").WithField("handle_id", payload.HandleID).Write()
						return
					}
					gap -= ch
				}
				pos = rStart
			}

			cur := rStart
			for cur < rEnd {
				ch := int64(len(zeroBuf))
				if rEnd-cur < ch {
					ch = rEnd - cur
				}
				buf := zeroBuf[:ch]
				n, rerr := ov.ReadAt(buf, cur)
				if rerr != nil && rerr != io.EOF {
					syslog.L.Error(rerr).
						WithMessage("handleReadAt: overlapped read error").WithField("handle_id", payload.HandleID).WithField("offset", cur).WithField("size", ch).Write()
					return
				}
				if n > 0 {
					if err := write(buf[:n]); err != nil {
						syslog.L.Error(err).WithMessage("handleReadAt: stream write data failed").WithField("handle_id", payload.HandleID).Write()
						return
					}
					cur += int64(n)
				}
				if rerr == io.EOF && n == 0 {
					syslog.L.Debug().WithMessage("handleReadAt: EOF reached").WithField("handle_id", payload.HandleID).Write()
					return
				}
			}
			pos = rEnd
		}

		if pos < end {
			gap := end - pos
			for gap > 0 {
				if err := req.Context.Err(); err != nil {
					return
				}
				ch := int64(len(zeroBuf))
				if gap < ch {
					ch = gap
				}
				if err := write(zeroBuf[:ch]); err != nil {
					syslog.L.Error(err).WithMessage("handleReadAt: stream write tail gap failed").WithField("handle_id", payload.HandleID).Write()
					return
				}
				gap -= ch
			}
		}
		syslog.L.Debug().WithMessage("handleReadAt: stream completed").WithField("handle_id", payload.HandleID).WithField("bytes", reqLen).Write()
	}

	syslog.L.Debug().WithMessage("handleReadAt: starting stream").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("length", payload.Length).Write()
	return arpc.Response{
		Status:    213,
		RawStream: streamFn,
	}, nil
}

func (s *AgentFSServer) handleLseek(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleLseek: decoding request").Write()
	var payload types.LseekReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		syslog.L.Debug().WithMessage("handleLseek: invalid whence").WithField("whence", payload.Whence).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleLseek: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		syslog.L.Debug().WithMessage("handleLseek: attempted lseek on directory").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh.Lock()
	defer fh.Unlock()

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(fh.handle, &std); err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: getFileStandardInfoByHandle failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	fileSize := std.EndOfFile

	cur := fh.logicalOffset
	var newOffset int64

	switch payload.Whence {
	case io.SeekStart:
		if payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative offset with SeekStart").WithField("offset", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = payload.Offset

	case io.SeekCurrent:
		if cur+payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative resulting offset with SeekCurrent").WithField("current", cur).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = cur + payload.Offset

	case io.SeekEnd:
		if fileSize+payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative resulting offset with SeekEnd").WithField("file_size", fileSize).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = fileSize + payload.Offset

	case SeekData, SeekHole:
		no, err := sparseSeekAllocatedRanges(
			fh.handle,
			payload.Offset,
			payload.Whence,
			fileSize,
		)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleLseek: sparseSeekAllocatedRanges failed").WithField("handle_id", payload.HandleID).Write()
			return arpc.Response{}, err
		}
		newOffset = no

	default:
		syslog.L.Debug().WithMessage("handleLseek: invalid whence in default").Write()
		return arpc.Response{}, os.ErrInvalid
	}

	if newOffset > fileSize {
		syslog.L.Debug().WithMessage("handleLseek: newOffset beyond EOF").WithField("new_offset", newOffset).WithField("file_size", fileSize).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh.logicalOffset = newOffset

	resp := types.LseekResp{NewOffset: newOffset}
	respBytes, err := cbor.Marshal(resp)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: encode failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleLseek: success").WithField("handle_id", payload.HandleID).WithField("new_offset", newOffset).Write()
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleClose(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleClose: decoding request").Write()
	var payload types.CloseReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleClose: decode failed").Write()
		return arpc.Response{}, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleClose: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	handle.Lock()
	defer handle.Unlock()

	if handle.mapping != 0 {
		windows.CloseHandle(handle.mapping)
		handle.mapping = 0
		syslog.L.Debug().WithMessage("handleClose: mapping closed").WithField("handle_id", payload.HandleID).Write()
	}
	if !handle.isDir {
		if handle.ov != nil {
			_ = handle.ov.Close()
			handle.ov = nil
			syslog.L.Debug().WithMessage("handleClose: overlapped closed").WithField("handle_id", payload.HandleID).Write()
		}
		windows.CloseHandle(handle.handle)
		syslog.L.Debug().WithMessage("handleClose: file handle closed").WithField("handle_id", payload.HandleID).Write()
	} else {
		if handle.dirReader != nil {
			handle.dirReader.Close()
		}
		if handle.ov != nil {
			_ = handle.ov.Close()
			handle.ov = nil
			syslog.L.Debug().WithMessage("handleClose: dir overlapped closed").WithField("handle_id", payload.HandleID).Write()
		}
		windows.CloseHandle(handle.handle)
		syslog.L.Debug().WithMessage("handleClose: dir handle closed").WithField("handle_id", payload.HandleID).Write()
	}

	s.handles.Del(uint64(payload.HandleID))

	closed := "closed"
	data, err := cbor.Marshal(closed)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleClose: encode close message failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleClose: success").WithField("handle_id", payload.HandleID).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}
