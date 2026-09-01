//go:build linux

package arpcfs

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/klauspost/compress/flate"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
)

const (
	zipMaxEntries    = 10000
	zipBombRatio     = 1000
	zipBombFloor     = 4 << 20
	zipRingSize      = 1 << 20
	zipSrcBufSize    = 1 << 19
	zipTailMax       = 65557
	zipCentralBudget = 64 << 20
)

var (
	errZipTooMany     = errors.New("zip has too many entries")
	errZipBomb        = errors.New("zip expansion ratio exceeds limit")
	errZipUnsupported = errors.New("zip uses unsupported feature")
	errZipCorrupt     = errors.New("zip is corrupt")
)

type zipEntry struct {
	name       string
	method     uint16
	compSize   int64
	uncompSize int64
	hdrOffset  int64
	dataOff    int64
	mode       uint32
	mtime      int64
	isDir      bool
}

type zipDir struct {
	children []zipChild
	mode     uint32
	mtime    int64
}

// zipChild references a file entry (entry >= 0) or a virtual dir via dirs (entry < 0).
type zipChild struct {
	name  string
	entry int32
}

// zipOverlay spills one expanded zip's contents into the anchor directory
// holding the zip. Registered overlays are never evicted, so walks never see
// disappearing subtrees; the budget instead gates new expansions.
type zipOverlay struct {
	zipPath   string
	size      int64
	entries   []zipEntry
	byName    map[string]int32
	dirs      map[string]*zipDir
	nameBytes int
	uncompSum int64
	readAt    func(ctx context.Context, p []byte, off int64) (int, error)
	src       *ARPCFile
	mu        sync.Mutex
	hdrBuf    [30]byte
}

// zipFileState is the per-open read state of a virtual file: store entries
// pass reads straight through; deflate entries keep one forward-only inflater
// with a ring, so no disk and bounded memory per open file.
// ponytail: backward reads before ringStart restart inflation from entry start.
type zipFileState struct {
	fs     *ARPCFS
	ov     *zipOverlay
	ent    *zipEntry
	uncomp int64

	ring      []byte
	ringStart int64
	fed       int64
	eof       bool
	sec       *io.SectionReader
	br        *bufio.Reader
	fr        io.ReadCloser
}

type zipSrc struct {
	readAt func(ctx context.Context, p []byte, off int64) (int, error)
}

func (s zipSrc) ReadAt(p []byte, off int64) (int, error) {
	return s.readAt(context.Background(), p, off)
}

func hasZipExt(name string) bool {
	return strings.EqualFold(path.Ext(name), ".zip")
}

func cleanZipName(raw string) (string, bool, bool) {
	s := strings.ReplaceAll(raw, "\\", "/")
	s = strings.TrimPrefix(s, "./")
	s = strings.TrimPrefix(s, "/")
	isDir := strings.HasSuffix(s, "/")
	s = strings.TrimSuffix(s, "/")
	if s == "" || s == "." {
		return "", false, false
	}
	for seg := range strings.SplitSeq(s, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", false, false
		}
	}
	return s, isDir, true
}

func dosTime(d, t uint16) int64 {
	if d == 0 && t == 0 {
		return 0
	}
	year := 1980 + int(d>>9)
	month := int((d >> 5) & 0xF)
	day := int(d & 0x1F)
	hour := int(t >> 11)
	min := int((t >> 5) & 0x3F)
	sec := int(t&0x1F) * 2
	return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC).Unix()
}

func zipMode(versionMadeBy uint16, extAttrs uint32) (perm uint32, isDir bool, isSymlink bool) {
	if versionMadeBy>>8 == 3 {
		m := extAttrs >> 16
		perm = m & 0o777
		switch m & 0o170000 {
		case 0o040000:
			isDir = true
		case 0o120000:
			isSymlink = true
		}
		if perm == 0 {
			perm = 0o644
			if isDir {
				perm = 0o755
			}
		}
		return perm, isDir, isSymlink
	}
	isDir = extAttrs&0x10 != 0
	if isDir {
		return 0o755, true, false
	}
	return 0o644, false, false
}

// parseZipOverlay applies gates then parses the whole central dir in one read.
func parseZipOverlay(readAt func(ctx context.Context, p []byte, off int64) (int, error), size int64) (*zipOverlay, error) {
	tailLen := min(size, int64(zipTailMax))
	tail := make([]byte, tailLen)
	if _, err := readAt(context.Background(), tail, size-tailLen); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	eocd := -1
	for i := len(tail) - 22; i >= 0; i-- {
		if tail[i] == 'P' && tail[i+1] == 'K' && tail[i+2] == 0x05 && tail[i+3] == 0x06 {
			eocd = i
			break
		}
	}
	if eocd < 0 {
		return nil, errZipCorrupt
	}

	totalEntries := int(binary.LittleEndian.Uint16(tail[eocd+10:]))
	cdSize := int64(binary.LittleEndian.Uint32(tail[eocd+12:]))
	cdOffset := int64(binary.LittleEndian.Uint32(tail[eocd+16:]))

	if totalEntries == 0xFFFF || cdSize == 0xFFFFFFFF || cdOffset == 0xFFFFFFFF {
		loc := eocd - 20
		if loc < 0 || binary.LittleEndian.Uint32(tail[loc:]) != 0x07064b50 {
			return nil, errZipCorrupt
		}
		z64Off := int64(binary.LittleEndian.Uint64(tail[loc+8:]))
		z64 := make([]byte, 56)
		if _, err := readAt(context.Background(), z64, z64Off); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if binary.LittleEndian.Uint32(z64) != 0x06064b50 {
			return nil, errZipCorrupt
		}
		totalEntries = int(binary.LittleEndian.Uint64(z64[32:]))
		cdSize = int64(binary.LittleEndian.Uint64(z64[40:]))
		cdOffset = int64(binary.LittleEndian.Uint64(z64[48:]))
	}

	if totalEntries > zipMaxEntries {
		return nil, fmt.Errorf("%w: %d entries exceeds %d", errZipTooMany, totalEntries, zipMaxEntries)
	}
	if cdSize <= 0 || cdOffset < 0 || cdOffset+cdSize > size {
		return nil, errZipCorrupt
	}

	cd := make([]byte, cdSize)
	if _, err := readAt(context.Background(), cd, cdOffset); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	ov := &zipOverlay{
		size:   size,
		readAt: readAt,
		byName: make(map[string]int32, totalEntries),
		dirs:   map[string]*zipDir{"": {}},
	}

	off := 0
	for i := 0; i < totalEntries; i++ {
		if off+46 > len(cd) || binary.LittleEndian.Uint32(cd[off:]) != 0x02014b50 {
			return nil, errZipCorrupt
		}
		versionMadeBy := binary.LittleEndian.Uint16(cd[off+4:])
		flags := binary.LittleEndian.Uint16(cd[off+8:])
		method := binary.LittleEndian.Uint16(cd[off+10:])
		mtime := dosTime(binary.LittleEndian.Uint16(cd[off+12:]), binary.LittleEndian.Uint16(cd[off+14:]))
		compSize := int64(binary.LittleEndian.Uint32(cd[off+20:]))
		uncompSize := int64(binary.LittleEndian.Uint32(cd[off+24:]))
		nameLen := int(binary.LittleEndian.Uint16(cd[off+28:]))
		extraLen := int(binary.LittleEndian.Uint16(cd[off+30:]))
		commentLen := int(binary.LittleEndian.Uint16(cd[off+32:]))
		extAttrs := binary.LittleEndian.Uint32(cd[off+38:])
		hdrOffset := int64(binary.LittleEndian.Uint32(cd[off+42:]))

		if off+46+nameLen+extraLen+commentLen > len(cd) {
			return nil, errZipCorrupt
		}
		rawName := string(cd[off+46 : off+46+nameLen])

		extraOff := off + 46 + nameLen
		extraEnd := extraOff + extraLen
		for extraOff+4 <= extraEnd {
			fieldStart := extraOff
			fieldID := binary.LittleEndian.Uint16(cd[extraOff:])
			fieldLen := int(binary.LittleEndian.Uint16(cd[extraOff+2:]))
			extraOff += 4 + fieldLen
			if extraOff > extraEnd {
				return nil, errZipCorrupt
			}
			if fieldID != 1 {
				continue
			}
			p := fieldStart + 4
			end := fieldStart + 4 + fieldLen
			if uncompSize == 0xFFFFFFFF && p+8 <= end {
				uncompSize = int64(binary.LittleEndian.Uint64(cd[p:]))
				p += 8
			}
			if compSize == 0xFFFFFFFF && p+8 <= end {
				compSize = int64(binary.LittleEndian.Uint64(cd[p:]))
				p += 8
			}
			if hdrOffset == 0xFFFFFFFF && p+8 <= end {
				hdrOffset = int64(binary.LittleEndian.Uint64(cd[p:]))
			}
		}

		off += 46 + nameLen + extraLen + commentLen

		if flags&1 != 0 {
			return nil, fmt.Errorf("%w: %s is encrypted", errZipUnsupported, rawName)
		}
		if method != 0 && method != 8 {
			return nil, fmt.Errorf("%w: %s uses method %d", errZipUnsupported, rawName, method)
		}

		name, namedDir, ok := cleanZipName(rawName)
		if !ok {
			continue
		}
		perm, modeDir, modeSymlink := zipMode(versionMadeBy, extAttrs)
		isDir := namedDir || modeDir

		if isDir {
			if _, exists := ov.byName[name]; exists {
				continue
			}
			ov.ensureDir(name, perm, mtime)
			continue
		}
		if _, exists := ov.byName[name]; exists {
			continue
		}
		if _, exists := ov.dirs[name]; exists {
			continue
		}

		mode := uint32(perm)
		if modeSymlink {
			mode |= uint32(os.ModeSymlink)
		}
		idx := int32(len(ov.entries))
		ov.entries = append(ov.entries, zipEntry{
			name:       name,
			method:     method,
			compSize:   compSize,
			uncompSize: uncompSize,
			hdrOffset:  hdrOffset,
			dataOff:    -1,
			mode:       mode,
			mtime:      mtime,
		})
		ov.byName[name] = idx
		ov.uncompSum += uncompSize
		ov.nameBytes += len(name)
		parent := ov.ensureParent(name)
		parent.children = append(parent.children, zipChild{name: baseName(name), entry: idx})
	}

	if ov.uncompSum > zipBombFloor && ov.uncompSum > size*zipBombRatio {
		return nil, fmt.Errorf("%w: %d/%d", errZipBomb, ov.uncompSum, size)
	}
	return ov, nil
}

func baseName(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

// ensureDir creates dir plus ancestor chain unless a file owns the name.
func (ov *zipOverlay) ensureDir(name string, mode uint32, mtime int64) *zipDir {
	if d, ok := ov.dirs[name]; ok {
		if mode != 0 {
			d.mode = mode
			d.mtime = mtime
		}
		return d
	}
	d := &zipDir{mode: mode, mtime: mtime}
	if d.mode == 0 {
		d.mode = 0o755
	}
	ov.dirs[name] = d
	if name != "" {
		parent := ov.ensureParent(name)
		parent.children = append(parent.children, zipChild{name: baseName(name), entry: -1})
	}
	return d
}

func (ov *zipOverlay) ensureParent(name string) *zipDir {
	i := strings.LastIndexByte(name, '/')
	if i < 0 {
		return ov.ensureDir("", 0, 0)
	}
	return ov.ensureDir(name[:i], 0, 0)
}

func (ov *zipOverlay) entryAttr(fullPath string, idx int32) fswire.AgentFileInfo {
	e := &ov.entries[idx]
	return fswire.AgentFileInfo{
		Name:           baseName(e.name),
		Size:           e.uncompSize,
		Mode:           e.mode,
		ModTime:        e.mtime * int64(time.Second),
		IsDir:          e.isDir,
		Blocks:         uint64((e.uncompSize + 511) >> 9),
		CreationTime:   e.mtime,
		LastAccessTime: e.mtime,
		LastWriteTime:  e.mtime,
	}
}

func (ov *zipOverlay) dirAttr(d *zipDir) fswire.AgentFileInfo {
	return fswire.AgentFileInfo{
		Mode:           d.mode,
		IsDir:          true,
		CreationTime:   d.mtime,
		LastAccessTime: d.mtime,
		LastWriteTime:  d.mtime,
	}
}

// dataOffset resolves the data start from the local header, cached on entry.
func (ov *zipOverlay) dataOffset(ctx context.Context, e *zipEntry) (int64, error) {
	ov.mu.Lock()
	defer ov.mu.Unlock()
	if e.dataOff >= 0 {
		return e.dataOff, nil
	}
	hdr := ov.hdrBuf[:]
	if _, err := ov.readAt(ctx, hdr, e.hdrOffset); err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if binary.LittleEndian.Uint32(hdr) != 0x04034b50 {
		return 0, errZipCorrupt
	}
	nameLen := int64(binary.LittleEndian.Uint16(hdr[26:]))
	extraLen := int64(binary.LittleEndian.Uint16(hdr[28:]))
	e.dataOff = e.hdrOffset + 30 + nameLen + extraLen
	return e.dataOff, nil
}

func (zs *zipFileState) init(ctx context.Context) error {
	off, err := zs.ov.dataOffset(ctx, zs.ent)
	if err != nil {
		return err
	}
	if zs.ent.method == 0 {
		return nil
	}
	zs.sec = io.NewSectionReader(zipSrc{zs.ov.readAt}, off, zs.ent.compSize)
	zs.br = bufio.NewReaderSize(zs.sec, zipSrcBufSize)
	zs.fr = flate.NewReader(zs.br)
	zs.ring = make([]byte, zipRingSize)
	return nil
}

func (zs *zipFileState) restart() {
	zs.sec.Seek(0, io.SeekStart)
	zs.br.Reset(zs.sec)
	if r, ok := zs.fr.(flate.Resetter); ok {
		r.Reset(zs.br, nil)
	}
	zs.ringStart = 0
	zs.fed = 0
	zs.eof = false
}

func (zs *zipFileState) fill(end int64) error {
	if end > zs.uncomp {
		end = zs.uncomp
	}
	for zs.fed < end && !zs.eof {
		pos := zs.fed - zs.ringStart
		if int(pos) == len(zs.ring) {
			keep := len(zs.ring) / 2
			copy(zs.ring, zs.ring[len(zs.ring)-keep:])
			zs.ringStart = zs.fed - int64(keep)
			pos = int64(keep)
		}
		n, err := zs.fr.Read(zs.ring[pos:])
		zs.fed += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				zs.eof = true
			} else {
				return err
			}
		}
	}
	return nil
}

func (zs *zipFileState) ReadAt(ctx context.Context, dest []byte, off int64) (int, error) {
	if off >= zs.uncomp {
		return 0, io.EOF
	}
	if zs.ent.method == 0 {
		dataOff, err := zs.ov.dataOffset(ctx, zs.ent)
		if err != nil {
			return 0, err
		}
		end := min(off+int64(len(dest)), zs.uncomp)
		n := int(end - off)
		m, err := zs.ov.readAt(ctx, dest[:n], dataOff+off)
		if err != nil && !errors.Is(err, io.EOF) {
			return m, err
		}
		if m < n {
			return m, io.EOF
		}
		return m, nil
	}

	if zs.ring == nil {
		if err := zs.init(ctx); err != nil {
			return 0, err
		}
	}
	if off < zs.ringStart {
		zs.restart()
	}
	if err := zs.fill(off + int64(len(dest))); err != nil {
		return 0, err
	}
	avail := zs.fed - off
	if avail <= 0 {
		return 0, io.EOF
	}
	n := min(int64(len(dest)), avail)
	start := off - zs.ringStart
	copy(dest, zs.ring[start:start+n])
	if off+n >= zs.uncomp {
		return int(n), io.EOF
	}
	return int(n), nil
}

func (zs *zipFileState) Lseek(off uint64, whence uint32) uint64 {
	size := uint64(zs.uncomp)
	switch whence {
	case io.SeekStart:
		if off > size {
			return size
		}
		return off
	case io.SeekCurrent, io.SeekEnd:
		return size
	case 3:
		if off >= size {
			return size
		}
		return off
	case 4:
		return size
	}
	return size
}

// zipAttr answers hidden zips and virtual paths; a miss must fall through to
// the agent so real files under an anchor keep working.
func (fs *ARPCFS) zipAttr(filename string) (fswire.AgentFileInfo, error, bool) {
	fs.zipMu.RLock()
	if _, hidden := fs.zipOverlays[filename]; hidden {
		fs.zipMu.RUnlock()
		return fswire.AgentFileInfo{}, syscall.ENOENT, true
	}
	for i := len(filename) - 1; i > 0; i-- {
		if filename[i] != '/' {
			continue
		}
		ovs := fs.zipAnchors[filename[:i]]
		if len(ovs) == 0 {
			continue
		}
		inner := filename[i+1:]
		for _, ov := range ovs {
			if idx, ok := ov.byName[inner]; ok {
				fs.zipMu.RUnlock()
				return ov.entryAttr(filename, idx), nil, true
			}
		}
		for _, ov := range ovs {
			if d, ok := ov.dirs[inner]; ok {
				fs.zipMu.RUnlock()
				return ov.dirAttr(d), nil, true
			}
		}
	}
	fs.zipMu.RUnlock()
	return fswire.AgentFileInfo{}, nil, false
}

func (fs *ARPCFS) zipHidden(fullPath string) bool {
	fs.zipMu.RLock()
	_, ok := fs.zipOverlays[fullPath]
	fs.zipMu.RUnlock()
	return ok
}

// zipProbe decides expansion once per zip; demotions are negatively cached.
func (fs *ARPCFS) zipProbe(ctx context.Context, fullPath string, size int64) bool {
	fs.zipMu.RLock()
	_, expanded := fs.zipOverlays[fullPath]
	_, skipped := fs.zipSkipped[fullPath]
	fs.zipMu.RUnlock()
	if expanded || skipped {
		return expanded
	}

	src, err := fs.Open(ctx, fullPath)
	if err != nil {
		fs.zipMu.Lock()
		if fs.zipSkipped == nil {
			fs.zipSkipped = map[string]struct{}{}
		}
		fs.zipSkipped[fullPath] = struct{}{}
		fs.zipMu.Unlock()
		return false
	}

	ov, err := parseZipOverlay(src.ReadAt, size)
	if err != nil {
		src.Close(ctx)
		fs.zipMu.Lock()
		if fs.zipSkipped == nil {
			fs.zipSkipped = map[string]struct{}{}
		}
		fs.zipSkipped[fullPath] = struct{}{}
		fs.zipMu.Unlock()
		fs.logOnce(fullPath, err, "zipExpand")
		return false
	}

	ov.zipPath = fullPath
	ov.src = src

	fs.zipMu.Lock()
	est := int64(len(ov.entries)*96 + ov.nameBytes)
	if fs.zipOverlays == nil {
		fs.zipOverlays = map[string]*zipOverlay{}
		fs.zipAnchors = map[string][]*zipOverlay{}
	}
	if fs.zipBytes+est > zipCentralBudget {
		fs.zipMu.Unlock()
		src.Close(ctx)
		fs.logOnce(fullPath, fmt.Errorf("%w: central dir budget %d exceeded", errZipUnsupported, zipCentralBudget), "zipExpand")
		return false
	}
	if _, dup := fs.zipOverlays[fullPath]; dup {
		fs.zipMu.Unlock()
		src.Close(ctx)
		return true
	}
	anchor := path.Dir(fullPath)
	fs.zipOverlays[fullPath] = ov
	fs.zipAnchors[anchor] = append(fs.zipAnchors[anchor], ov)
	fs.zipBytes += est
	fs.zipMu.Unlock()
	return true
}

func (fs *ARPCFS) zipOpen(ctx context.Context, filename string) (*ARPCFile, bool) {
	fs.zipMu.RLock()
	for i := len(filename) - 1; i > 0; i-- {
		if filename[i] != '/' {
			continue
		}
		ovs := fs.zipAnchors[filename[:i]]
		if len(ovs) == 0 {
			continue
		}
		inner := filename[i+1:]
		for _, ov := range ovs {
			if idx, ok := ov.byName[inner]; ok {
				fs.zipMu.RUnlock()
				e := &ov.entries[idx]
				return &ARPCFile{
					fs:   fs,
					name: filename,
					zs: &zipFileState{
						fs:     fs,
						ov:     ov,
						ent:    e,
						uncomp: e.uncompSize,
					},
				}, true
			}
		}
	}
	fs.zipMu.RUnlock()
	return nil, false
}

type zipVChild struct {
	ov    *zipOverlay
	child zipChild
}

// zipCollectChildren merges root children of overlays anchored at dir with
// subtree children of overlays anchored above dir.
func (fs *ARPCFS) zipCollectChildren(dir string) []zipVChild {
	fs.zipMu.RLock()
	defer fs.zipMu.RUnlock()

	var out []zipVChild
	for _, ov := range fs.zipAnchors[dir] {
		if d := ov.dirs[""]; d != nil {
			for _, c := range d.children {
				out = append(out, zipVChild{ov, c})
			}
		}
	}
	for i := len(dir) - 1; i > 0; i-- {
		if dir[i] != '/' {
			continue
		}
		inner := dir[i+1:]
		for _, ov := range fs.zipAnchors[dir[:i]] {
			if d := ov.dirs[inner]; d != nil {
				for _, c := range d.children {
					out = append(out, zipVChild{ov, c})
				}
			}
		}
	}
	return out
}

func (fs *ARPCFS) zipShutdown(ctx context.Context) {
	fs.zipMu.Lock()
	ovs := fs.zipOverlays
	fs.zipOverlays = nil
	fs.zipAnchors = nil
	fs.zipSkipped = nil
	fs.zipBytes = 0
	fs.zipMu.Unlock()
	for _, ov := range ovs {
		if ov.src != nil {
			ov.src.Close(ctx)
		}
	}
}

// zipReaddir drains the agent stream first (probing and hiding zips on
// encounter), then appends overlay children; real entries shadow virtual ones.
func (fs *ARPCFS) zipReaddir(ctx context.Context, dirPath string) (fs.DirStream, bool) {
	virtual := fs.zipCollectChildren(dirPath)

	var agent *DirStream
	if stream, err := fs.ReadDir(ctx, dirPath); err == nil {
		agent = &stream
	} else if len(virtual) == 0 {
		return nil, false
	}

	return &zipMergeStream{
		fs:    fs,
		path:  dirPath,
		agent: agent,
	}, true
}

type zipMergeStream struct {
	fs        *ARPCFS
	path      string
	agent     *DirStream
	mu        sync.Mutex
	agentDone bool
	vqueue    []zipVChild
	vidx      int
	emitted   map[string]struct{}
	pending   *fuse.DirEntry
	closed    bool
}

func (s *zipMergeStream) HasNext() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	if !s.agentDone {
		if s.agent != nil {
			for s.agent.HasNext() {
				e, errno := s.agent.Next()
				if errno != 0 {
					continue
				}
				full := s.path + "/" + e.Name
				if hasZipExt(e.Name) {
					if s.fs.zipHidden(full) {
						continue
					}
					if fi, err := s.fs.Attr(s.fs.Ctx, full, true); err == nil && !fi.IsDir && fi.Size >= 22 {
						if s.fs.zipProbe(s.fs.Ctx, full, fi.Size) {
							continue
						}
					}
					s.markEmitted(e.Name)
					s.pending = &e
					return true
				}
				s.markEmitted(e.Name)
				s.pending = &e
				return true
			}
			s.agent.Close()
		}
		s.agentDone = true
		s.vqueue = s.fs.zipCollectChildren(s.path)
		if len(s.vqueue) > 0 && s.emitted == nil {
			s.emitted = map[string]struct{}{}
		}
	}

	for s.vidx < len(s.vqueue) {
		vc := s.vqueue[s.vidx]
		s.vidx++
		if _, shadowed := s.emitted[vc.child.name]; shadowed {
			continue
		}
		mode := uint32(fuse.S_IFREG)
		if vc.child.entry < 0 {
			mode = fuse.S_IFDIR
		}
		s.emitted[vc.child.name] = struct{}{}
		entry := fuse.DirEntry{Name: vc.child.name, Mode: mode}
		s.pending = &entry
		return true
	}
	return false
}

func (s *zipMergeStream) markEmitted(name string) {
	if s.emitted != nil {
		s.emitted[name] = struct{}{}
	}
}

func (s *zipMergeStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.pending == nil {
		return fuse.DirEntry{}, syscall.EBADF
	}
	e := *s.pending
	s.pending = nil

	if e.Mode&uint32(fuse.S_IFDIR) != 0 {
		s.fs.FolderCount.Add(1)
	} else {
		s.fs.FileCount.Add(1)
	}
	return e, 0
}

func (s *zipMergeStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	if s.agent != nil && !s.agentDone {
		s.agent.Close()
	}
}
