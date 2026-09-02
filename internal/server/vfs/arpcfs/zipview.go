//go:build linux

package arpcfs

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bodgit/sevenzip"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/klauspost/compress/flate"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	extraExtendedTime = 0x5455
	extraNTFSTime     = 0x000a

	zipMaxEntries    = 10000
	zipBombRatio     = 1000
	zipBombFloor     = 4 << 20
	zipRingSize      = 1 << 20
	zipSrcBufSize    = 1 << 19
	zipTailMax       = 65557
	zipCentralBudget = 64 << 20
	zipSymlinkMax    = 4096
)

var (
	errZipTooMany     = errors.New("archive has too many entries")
	errZipBomb        = errors.New("archive expansion ratio exceeds limit")
	errZipUnsupported = errors.New("archive uses unsupported feature")
	errZipCorrupt     = errors.New("archive is corrupt")
)

type zipEntry struct {
	name       string
	method     uint16
	crc        uint32
	compSize   int64
	uncompSize int64
	hdrOffset  int64
	dataOff    int64
	sidx       int32
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
	zipPath       string
	parentZipPath string
	size          int64
	entries       []zipEntry
	sfiles        []*sevenzip.File
	byName        map[string]int32
	dirs          map[string]*zipDir
	nameBytes     int
	uncompSum     int64
	entryCount    int64
	readAt        func(ctx context.Context, p []byte, off int64) (int, error)
	src           *ARPCFile
	cleanup       func()
	mu            sync.Mutex
	hdrBuf        [30]byte
}

// zipFileState is the per-open read state of a virtual file: store entries
// pass reads straight through; deflate entries keep one forward-only inflater
// with a ring, so no disk and bounded memory per open file.
// ponytail: backward reads before ringStart restart inflation from entry start.
type zipFileState struct {
	mu     sync.Mutex
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
	crc       uint32
	crcFed    int64
	crcBad    bool
	crcDone   bool
}

type zipSrc struct {
	readAt func(ctx context.Context, p []byte, off int64) (int, error)
}

func (s zipSrc) ReadAt(p []byte, off int64) (int, error) {
	return s.readAt(context.Background(), p, off)
}

func (fs *ARPCFS) archiveEnabled(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".zip":
		return fs.expandZip
	case ".7z":
		return fs.expandSevenZip
	default:
		return false
	}
}

func expansionTooLarge(uncompressed, compressed int64) bool {
	if uncompressed <= zipBombFloor {
		return false
	}
	return compressed <= 0 || uncompressed/compressed > zipBombRatio ||
		uncompressed/compressed == zipBombRatio && uncompressed%compressed != 0
}

// anchorKey maps the empty parent of a root-level path to the "/" anchor.
func anchorKey(dir string) string {
	if dir == "" {
		return "/"
	}
	return dir
}

func joinPath(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
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

// dosTime decodes the archiver's local wall clock; fallback only, 2s resolution.
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
func parseZipOverlay(readAt func(ctx context.Context, p []byte, off int64) (int, error), size, maxEntries int64) (*zipOverlay, error) {
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

	if int64(totalEntries) > maxEntries {
		return nil, fmt.Errorf("%w: %d entries exceeds %d", errZipTooMany, totalEntries, maxEntries)
	}
	if cdSize <= 0 || cdOffset < 0 || cdOffset+cdSize > size {
		return nil, errZipCorrupt
	}

	cd := make([]byte, cdSize)
	if _, err := readAt(context.Background(), cd, cdOffset); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	ov := &zipOverlay{
		size:       size,
		entryCount: int64(totalEntries),
		readAt:     readAt,
		byName:     make(map[string]int32, totalEntries),
		dirs:       map[string]*zipDir{"": {}},
	}

	off := 0
	for i := 0; i < totalEntries; i++ {
		if off+46 > len(cd) || binary.LittleEndian.Uint32(cd[off:]) != 0x02014b50 {
			return nil, errZipCorrupt
		}
		versionMadeBy := binary.LittleEndian.Uint16(cd[off+4:])
		flags := binary.LittleEndian.Uint16(cd[off+8:])
		method := binary.LittleEndian.Uint16(cd[off+10:])
		mtime := dosTime(binary.LittleEndian.Uint16(cd[off+14:]), binary.LittleEndian.Uint16(cd[off+12:]))
		crc := binary.LittleEndian.Uint32(cd[off+16:])
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
		var extMtime int64
		for extraOff+4 <= extraEnd {
			fieldStart := extraOff
			fieldID := binary.LittleEndian.Uint16(cd[extraOff:])
			fieldLen := int(binary.LittleEndian.Uint16(cd[extraOff+2:]))
			extraOff += 4 + fieldLen
			if extraOff > extraEnd {
				return nil, errZipCorrupt
			}
			p := fieldStart + 4
			end := fieldStart + 4 + fieldLen
			switch fieldID {
			case 1:
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
			case extraExtendedTime:
				if extMtime == 0 && p < end && cd[p]&1 != 0 && p+5 <= end {
					extMtime = int64(int32(binary.LittleEndian.Uint32(cd[p+1:])))
				}
			case extraNTFSTime:
				if extMtime == 0 && p+4+4+8 <= end &&
					binary.LittleEndian.Uint16(cd[p+4:]) == 1 {
					extMtime = filetimeUnix(binary.LittleEndian.Uint64(cd[p+8:]))
				}
			}
		}
		if extMtime != 0 {
			mtime = extMtime
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
			crc:        crc,
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

	ov.backfillDirMtimes()
	if expansionTooLarge(ov.uncompSum, size) {
		return nil, fmt.Errorf("%w: %d/%d", errZipBomb, ov.uncompSum, size)
	}
	return ov, nil
}

// filetimeUnix converts a Windows FILETIME (100ns ticks since 1601) to seconds.
func filetimeUnix(ft uint64) int64 {
	if ft == 0 {
		return 0
	}
	return int64(ft/10000000) - 11644473600
}

// backfillDirMtimes keeps implicit parents (no archive entry) off the epoch.
func (ov *zipOverlay) backfillDirMtimes() {
	names := make([]string, 0, len(ov.dirs))
	for name := range ov.dirs {
		if ov.dirs[name].mtime == 0 {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.Count(names[i], "/") > strings.Count(names[j], "/")
	})
	for _, name := range names {
		d := ov.dirs[name]
		for _, c := range d.children {
			var m int64
			if c.entry >= 0 {
				m = ov.entries[c.entry].mtime
			} else if cd, ok := ov.dirs[childDirName(name, c.name)]; ok {
				m = cd.mtime
			}
			if m > d.mtime {
				d.mtime = m
			}
		}
	}
}

func childDirName(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
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
		}
		if mtime != 0 {
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
		ModTime:        d.mtime * int64(time.Second),
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
	if zs.ent.method == m7z {
		fr, err := zs.ov.sfiles[zs.ent.sidx].Open()
		if err != nil {
			return fmt.Errorf("%w: %w", errZipUnsupported, err)
		}
		zs.fr = fr
		zs.ring = make([]byte, zipRingSize)
		return nil
	}
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

func (zs *zipFileState) restart() error {
	if zs.ent.method == m7z {
		zs.fr.Close()
		fr, err := zs.ov.sfiles[zs.ent.sidx].Open()
		if err != nil {
			return fmt.Errorf("%w: %w", errZipUnsupported, err)
		}
		zs.fr = fr
	} else {
		zs.sec.Seek(0, io.SeekStart)
		zs.br.Reset(zs.sec)
		if r, ok := zs.fr.(flate.Resetter); ok {
			r.Reset(zs.br, nil)
		}
	}
	zs.ringStart = 0
	zs.fed = 0
	zs.eof = false
	zs.crc = 0
	zs.crcFed = 0
	zs.crcBad = false
	zs.crcDone = false
	return nil
}

func (zs *zipFileState) checkCRC() error {
	if zs.ent.method == m7z || zs.crcDone {
		return nil
	}
	if zs.crcFed != zs.uncomp || zs.crc != zs.ent.crc {
		return fmt.Errorf("%w: CRC mismatch for %s", errZipCorrupt, zs.ent.name)
	}
	zs.crcDone = true
	return nil
}

func (zs *zipFileState) verifyStoredCRC(ctx context.Context, dataOff int64) error {
	buf := make([]byte, zipSrcBufSize)
	var sum uint32
	var off int64
	for off < zs.uncomp {
		n := int(min(int64(len(buf)), zs.uncomp-off))
		m, err := zs.ov.readAt(ctx, buf[:n], dataOff+off)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if m == 0 {
			return fmt.Errorf("%w: short stored entry %s", errZipCorrupt, zs.ent.name)
		}
		sum = crc32.Update(sum, crc32.IEEETable, buf[:m])
		off += int64(m)
	}
	zs.crc = sum
	zs.crcFed = off
	zs.crcBad = false
	return zs.checkCRC()
}

func (zs *zipFileState) fill(off, end int64) error {
	if end > zs.uncomp {
		end = zs.uncomp
	}
	for zs.fed < end && !zs.eof {
		pos := zs.fed - zs.ringStart
		if int(pos) == len(zs.ring) {
			keep := len(zs.ring) / 2
			if zs.fed-int64(keep) > off {
				keep = int(zs.fed - off)
			}
			copy(zs.ring, zs.ring[len(zs.ring)-keep:])
			zs.ringStart = zs.fed - int64(keep)
			pos = int64(keep)
		}
		start := int(pos)
		n, err := zs.fr.Read(zs.ring[start:])
		zs.fed += int64(n)
		if zs.ent.method != m7z && n > 0 {
			zs.crc = crc32.Update(zs.crc, crc32.IEEETable, zs.ring[start:start+n])
			zs.crcFed += int64(n)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			zs.eof = true
			if zs.fed < zs.uncomp {
				return fmt.Errorf("%w: short entry %s", errZipCorrupt, zs.ent.name)
			}
		}
		if zs.fed == zs.uncomp {
			if err := zs.checkCRC(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (zs *zipFileState) ReadAt(ctx context.Context, dest []byte, off int64) (int, error) {
	// FUSE readahead overlaps READs on one handle; ring and decoder state
	// are single-consumer, so serialize or the flate stream corrupts.
	zs.mu.Lock()
	defer zs.mu.Unlock()
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
		readEnd := off + int64(m)
		if !zs.crcBad {
			switch {
			case off > zs.crcFed:
				zs.crcBad = true
			case readEnd > zs.crcFed:
				start := int(zs.crcFed - off)
				zs.crc = crc32.Update(zs.crc, crc32.IEEETable, dest[start:m])
				zs.crcFed = readEnd
			}
		}
		if readEnd >= zs.uncomp {
			if zs.crcBad || zs.crcFed != zs.uncomp {
				if err := zs.verifyStoredCRC(ctx, dataOff); err != nil {
					return m, err
				}
			} else if err := zs.checkCRC(); err != nil {
				return m, err
			}
		}
		if m < n {
			return m, io.EOF
		}
		return m, nil
	}

	total := 0
	for total < len(dest) {
		if zs.ring == nil {
			if err := zs.init(ctx); err != nil {
				return total, err
			}
		}
		cur := off + int64(total)
		if cur < zs.ringStart {
			if err := zs.restart(); err != nil {
				return total, err
			}
		}
		chunk := min(len(zs.ring)/2, len(dest)-total)
		if err := zs.fill(cur, cur+int64(chunk)); err != nil {
			return total, err
		}
		avail := zs.fed - cur
		if avail <= 0 {
			return total, io.EOF
		}
		n := min(int64(chunk), avail)
		start := cur - zs.ringStart
		copy(dest[total:total+int(n)], zs.ring[start:start+n])
		total += int(n)
		if cur+int64(n) >= zs.uncomp {
			return total, io.EOF
		}
	}
	return total, nil
}

func (zs *zipFileState) verify(ctx context.Context) error {
	if zs.ent.method == m7z {
		return nil
	}
	zs.mu.Lock()
	done, bad := zs.crcDone, zs.crcBad
	zs.mu.Unlock()
	if bad {
		return errZipCorrupt
	}
	if done {
		return nil
	}
	zs.close()
	buf := make([]byte, 64<<10)
	for off := int64(0); off < zs.uncomp; {
		n, err := zs.ReadAt(ctx, buf, off)
		off += int64(n)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			return errZipCorrupt
		}
	}
	zs.mu.Lock()
	done, bad = zs.crcDone, zs.crcBad
	zs.mu.Unlock()
	if bad || !done {
		return errZipCorrupt
	}
	return nil
}

func (zs *zipFileState) close() {
	zs.mu.Lock()
	if zs.fr != nil {
		_ = zs.fr.Close()
	}
	zs.fr = nil
	zs.sec = nil
	zs.br = nil
	zs.ring = nil
	zs.ringStart = 0
	zs.fed = 0
	zs.eof = false
	zs.crc = 0
	zs.crcFed = 0
	zs.crcBad = false
	zs.crcDone = false
	zs.mu.Unlock()
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
	if _, shadowed := fs.zipShadowed[filename]; shadowed {
		fs.zipMu.RUnlock()
		return fswire.AgentFileInfo{}, nil, false
	}
	if _, hidden := fs.zipOverlays[filename]; hidden {
		fs.zipMu.RUnlock()
		return fswire.AgentFileInfo{}, syscall.ENOENT, true
	}
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] != '/' {
			continue
		}
		ovs := fs.zipAnchors[anchorKey(filename[:i])]
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

func (fs *ARPCFS) zipMarkShadowed(filename string) {
	if _, err, ok := fs.zipAttr(filename); !ok || err != nil {
		return
	}
	fs.zipMu.Lock()
	if fs.zipShadowed == nil {
		fs.zipShadowed = map[string]struct{}{}
	}
	fs.zipShadowed[filename] = struct{}{}
	fs.zipMu.Unlock()
}

func (fs *ARPCFS) zipHidden(fullPath string) bool {
	fs.zipMu.RLock()
	_, ok := fs.zipOverlays[fullPath]
	fs.zipMu.RUnlock()
	return ok
}

// warnOnce logs a one-time archive demotion notice; demotion is the
// expected gate outcome, not an error.
func (fs *ARPCFS) warnOnce(path string, err error) {
	if isIgnoredPath(path) {
		return
	}
	if _, loaded := fs.loggedPaths.LoadOrStore(path, struct{}{}); loaded {
		return
	}
	log.Warn("FUSE zipExpand demoted",
		"path", path, "error", err.Error())
}

func (fs *ARPCFS) collectNestedOverlays(ctx context.Context, root *zipOverlay, maxEntries int64) []*zipOverlay {
	type frame struct {
		overlay *zipOverlay
		depth   int
		next    int
	}

	remaining := maxEntries - root.entryCount
	visibleBytes := root.uncompSum
	stack := []frame{{overlay: root}}
	var nested []*zipOverlay
	for len(stack) > 0 {
		current := &stack[len(stack)-1]
		if current.next >= len(current.overlay.entries) {
			stack = stack[:len(stack)-1]
			continue
		}
		if ctx.Err() != nil {
			break
		}
		entry := &current.overlay.entries[current.next]
		parent := current.overlay
		depth := current.depth
		current.next++
		if os.FileMode(entry.mode)&os.ModeSymlink != 0 || !fs.archiveEnabled(entry.name) {
			continue
		}
		fullPath := joinPath(path.Dir(parent.zipPath), entry.name)
		if fs.expandMaxDepth >= 0 && depth >= fs.expandMaxDepth {
			fs.warnOnce(fullPath, fmt.Errorf("%w: nested archive depth exceeds %d", errZipUnsupported, fs.expandMaxDepth))
			continue
		}
		if remaining <= 0 {
			fs.warnOnce(fullPath, fmt.Errorf("%w: archive tree exceeds %d entries", errZipTooMany, maxEntries))
			continue
		}

		state := &zipFileState{fs: fs, ov: parent, ent: entry, uncomp: entry.uncompSize}
		child, err := parseArchiveOverlay(state.ReadAt, entry.uncompSize, remaining)
		if err == nil {
			err = state.verify(ctx)
		}
		state.close()
		if err != nil {
			fs.warnOnce(fullPath, err)
			continue
		}
		maxInt64 := int64(^uint64(0) >> 1)
		delta := child.uncompSum - entry.uncompSize
		if delta > 0 && visibleBytes > maxInt64-delta {
			fs.warnOnce(fullPath, errZipBomb)
			continue
		}
		nextVisible := visibleBytes + delta
		if expansionTooLarge(nextVisible, root.size) {
			fs.warnOnce(fullPath, fmt.Errorf("%w: nested tree %d/%d", errZipBomb, nextVisible, root.size))
			continue
		}

		child.zipPath = fullPath
		child.parentZipPath = parent.zipPath
		child.cleanup = state.close
		remaining -= child.entryCount
		visibleBytes = nextVisible
		nested = append(nested, child)
		stack = append(stack, frame{overlay: child, depth: depth + 1})
	}
	return nested
}

// zipProbe decides expansion once per zip; demotions are negatively cached.
func (fs *ARPCFS) zipProbe(ctx context.Context, fullPath string, size int64) bool {
	if !fs.archiveEnabled(fullPath) {
		return false
	}
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

	maxEntries := int64(zipMaxEntries)
	if fs.expandMaxEntries < 0 {
		maxEntries = int64(^uint64(0) >> 1)
	} else if fs.expandMaxEntries > 0 {
		maxEntries = int64(fs.expandMaxEntries)
	}
	ov, err := parseArchiveOverlay(src.ReadAt, size, maxEntries)
	if err != nil {
		src.Close(ctx)
		fs.zipMu.Lock()
		if fs.zipSkipped == nil {
			fs.zipSkipped = map[string]struct{}{}
		}
		fs.zipSkipped[fullPath] = struct{}{}
		fs.zipMu.Unlock()
		fs.warnOnce(fullPath, err)
		return false
	}

	ov.zipPath = fullPath
	ov.src = src
	overlays := append([]*zipOverlay{ov}, fs.collectNestedOverlays(ctx, ov, maxEntries)...)

	var est int64
	for _, overlay := range overlays {
		est += int64(len(overlay.entries)*96 + overlay.nameBytes)
	}
	fs.zipMu.Lock()
	if fs.zipOverlays == nil {
		fs.zipOverlays = map[string]*zipOverlay{}
		fs.zipAnchors = map[string][]*zipOverlay{}
	}
	if fs.zipBytes+est > zipCentralBudget {
		fs.zipMu.Unlock()
		src.Close(ctx)
		for _, nested := range overlays[1:] {
			nested.cleanup()
		}
		fs.warnOnce(fullPath, fmt.Errorf("%w: central dir budget %d exceeded", errZipUnsupported, zipCentralBudget))
		return false
	}
	if _, dup := fs.zipOverlays[fullPath]; dup {
		fs.zipMu.Unlock()
		src.Close(ctx)
		for _, nested := range overlays[1:] {
			nested.cleanup()
		}
		return true
	}
	blocked := map[string]bool{}
	for _, overlay := range overlays {
		if blocked[overlay.parentZipPath] {
			blocked[overlay.zipPath] = true
			if overlay.cleanup != nil {
				overlay.cleanup()
			}
			continue
		}
		if _, dup := fs.zipOverlays[overlay.zipPath]; dup {
			blocked[overlay.zipPath] = true
			if overlay.cleanup != nil {
				overlay.cleanup()
			}
			continue
		}
		anchor := path.Dir(overlay.zipPath)
		fs.zipOverlays[overlay.zipPath] = overlay
		fs.zipAnchors[anchor] = append(fs.zipAnchors[anchor], overlay)
		fs.zipBytes += int64(len(overlay.entries)*96 + overlay.nameBytes)
	}
	fs.zipMu.Unlock()
	return true
}

func (fs *ARPCFS) zipOpen(ctx context.Context, filename string) (*ARPCFile, bool) {
	fs.zipMu.RLock()
	if _, shadowed := fs.zipShadowed[filename]; shadowed {
		fs.zipMu.RUnlock()
		return nil, false
	}
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] != '/' {
			continue
		}
		ovs := fs.zipAnchors[anchorKey(filename[:i])]
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

func (fs *ARPCFS) zipReadlink(ctx context.Context, filename string) ([]byte, bool, error) {
	f, ok := fs.zipOpen(ctx, filename)
	if !ok || os.FileMode(f.zs.ent.mode)&os.ModeSymlink == 0 {
		return nil, false, nil
	}
	if f.zs.uncomp > zipSymlinkMax {
		return nil, true, fmt.Errorf("%w: symlink target exceeds %d bytes", errZipUnsupported, zipSymlinkMax)
	}
	target := make([]byte, f.zs.uncomp)
	n, err := f.zs.ReadAt(ctx, target, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, true, err
	}
	if int64(n) != f.zs.uncomp {
		return nil, true, fmt.Errorf("%w: short symlink target", errZipCorrupt)
	}
	return target, true, nil
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
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] != '/' {
			continue
		}
		inner := dir[i+1:]
		if inner == "" {
			continue
		}
		for _, ov := range fs.zipAnchors[anchorKey(dir[:i])] {
			if d := ov.dirs[inner]; d != nil {
				for _, c := range d.children {
					out = append(out, zipVChild{ov, c})
				}
			}
		}
	}
	return out
}

// zipIsVirtualDir reports whether dirPath names an overlay-only directory, even childless.
func (fs *ARPCFS) zipIsVirtualDir(dirPath string) bool {
	fs.zipMu.RLock()
	defer fs.zipMu.RUnlock()

	for i := len(dirPath) - 1; i >= 0; i-- {
		if dirPath[i] != '/' {
			continue
		}
		inner := dirPath[i+1:]
		if inner == "" {
			continue
		}
		for _, ov := range fs.zipAnchors[anchorKey(dirPath[:i])] {
			if _, ok := ov.dirs[inner]; ok {
				return true
			}
		}
	}
	return false
}

func (fs *ARPCFS) zipShutdown(ctx context.Context) {
	fs.zipMu.Lock()
	ovs := fs.zipOverlays
	fs.zipOverlays = nil
	fs.zipAnchors = nil
	fs.zipSkipped = nil
	fs.zipShadowed = nil
	fs.zipBytes = 0
	fs.zipMu.Unlock()
	for _, ov := range ovs {
		if ov.src != nil {
			ov.src.Close(ctx)
		}
		if ov.cleanup != nil {
			ov.cleanup()
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
	} else if len(virtual) == 0 && !fs.zipIsVirtualDir(dirPath) {
		return nil, false
	}

	return &zipMergeStream{
		fs:      fs,
		path:    dirPath,
		agent:   agent,
		emitted: map[string]struct{}{},
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
				full := joinPath(s.path, e.Name)
				if s.fs.archiveEnabled(e.Name) {
					// Attr hides expanded archives itself (post-stat probe returns
					// ENOENT), so an error here usually means hidden, not vanished.
					fi, aerr := s.fs.Attr(s.fs.Ctx, full, true)
					if aerr == nil && !fi.IsDir && fi.Size >= 32 && s.fs.zipProbe(s.fs.Ctx, full, fi.Size) {
						continue
					}
					if aerr != nil && s.fs.zipHidden(full) {
						continue
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
	}
	if s.emitted == nil {
		s.emitted = map[string]struct{}{}
	}

	for s.vidx < len(s.vqueue) {
		vc := s.vqueue[s.vidx]
		s.vidx++
		if _, shadowed := s.emitted[vc.child.name]; shadowed {
			continue
		}
		if s.fs.zipHidden(joinPath(s.path, vc.child.name)) {
			continue
		}
		mode := uint32(fuse.S_IFREG)
		if vc.child.entry < 0 {
			mode = fuse.S_IFDIR
		} else if os.FileMode(vc.ov.entries[vc.child.entry].mode)&os.ModeSymlink != 0 {
			mode = fuse.S_IFLNK
		}
		s.emitted[vc.child.name] = struct{}{}
		entry := fuse.DirEntry{Name: vc.child.name, Mode: mode}
		s.pending = &entry
		return true
	}
	return false
}

func (s *zipMergeStream) markEmitted(name string) {
	if s.emitted == nil {
		s.emitted = map[string]struct{}{}
	}
	s.emitted[name] = struct{}{}
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
