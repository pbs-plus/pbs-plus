package native

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/puzpuzpuz/xsync/v4"
)

var ErrNotFound = errors.New("not found")
var ErrNotDirectory = errors.New("not a directory")

type FileEntry struct {
	accessor   *Accessor
	entryRange EntryRangeInfo
	path       string
	name       string
	metadata   *Metadata
	kind       EntryKind
}

type EntryRangeInfo struct {
	EntryStart  uint64
	EntryEnd    uint64
	ContentInfo *ContentInfo
}

type ContentInfo struct {
	Start uint64
	End   uint64
}

type EntryKind int

const (
	EntryKindFile EntryKind = iota
	EntryKindDirectory
	EntryKindSymlink
	EntryKindHardlink
	EntryKindDevice
	EntryKindFifo
	EntryKindSocket
)

type Metadata struct {
	Stat    Stat
	XAttrs  []XAttr
	FCaps   []byte
	ACLUser []ACLUserEntry
	ACLGrp  []ACLGroupEntry
}

type ACLUserEntry struct {
	UID  uint32
	Perm uint64
}

type ACLGroupEntry struct {
	GID  uint32
	Perm uint64
}

type Accessor struct {
	metaReader    Reader
	payloadReader Reader
	metaSize      uint64
	payloadSize   uint64

	mu       sync.Mutex
	root     *FileEntry
	entries  map[uint64]*FileEntry
	dirCache *xsync.MapOf[uint64, []FileEntry]
}

type Reader interface {
	ReadAt(p []byte, offset int64) (n int, err error)
	Size() uint64
}

func NewAccessor(metaReader Reader, metaSize uint64, payloadReader Reader, payloadSize uint64) (*Accessor, error) {
	a := &Accessor{
		metaReader:    metaReader,
		payloadReader: payloadReader,
		metaSize:      metaSize,
		payloadSize:   payloadSize,
		entries:       make(map[uint64]*FileEntry),
		dirCache:      xsync.NewMapOf[uint64, []FileEntry](),
	}

	root, err := a.openRoot()
	if err != nil {
		return nil, fmt.Errorf("open root: %w", err)
	}

	a.root = root
	return a, nil
}

func (a *Accessor) openRoot() (*FileEntry, error) {
	header, _, err := a.readHeader(0)
	if err != nil {
		return nil, fmt.Errorf("read root header: %w", err)
	}

	if header.HType != PXARFormatVersion && header.HType != PXAREntry {
		return nil, fmt.Errorf("unexpected root header type: %x", header.HType)
	}

	offset := uint64(PxarHeaderSize)
	if header.HType == PXARFormatVersion {
		offset = header.FullSize
	}

	entry, err := a.readEntry(offset)
	if err != nil {
		return nil, fmt.Errorf("read root entry: %w", err)
	}

	return entry, nil
}

func (a *Accessor) readHeader(offset uint64) (*Header, []byte, error) {
	headerBytes := make([]byte, PxarHeaderSize)
	n, err := a.metaReader.ReadAt(headerBytes, int64(offset))
	if err != nil {
		return nil, nil, fmt.Errorf("read header at %d: %w", offset, err)
	}
	if n != int(PxarHeaderSize) {
		return nil, nil, fmt.Errorf("incomplete header read at %d", offset)
	}

	header := &Header{
		HType:    binary.LittleEndian.Uint64(headerBytes[0:8]),
		FullSize: binary.LittleEndian.Uint64(headerBytes[8:16]),
	}

	return header, headerBytes, nil
}

func (a *Accessor) readEntry(offset uint64) (*FileEntry, error) {
	a.mu.Lock()
	if entry, ok := a.entries[offset]; ok {
		a.mu.Unlock()
		return entry, nil
	}
	a.mu.Unlock()

	header, _, err := a.readHeader(offset)
	if err != nil {
		return nil, err
	}

	if header.HType != PXAREntry && header.HType != PXAREntryV1 {
		return nil, fmt.Errorf("not an entry header at %d: %x", offset, header.HType)
	}

	entryStart := offset
	entryEnd := offset + header.FullSize

	stat, err := a.readStat(offset+PxarHeaderSize, header.HType)
	if err != nil {
		return nil, fmt.Errorf("read stat: %w", err)
	}

	entry := &FileEntry{
		accessor: a,
		entryRange: EntryRangeInfo{
			EntryStart: entryStart,
			EntryEnd:   entryEnd,
		},
		metadata: &Metadata{Stat: *stat},
	}

	currentOffset := offset + PxarHeaderSize + a.statSize(header.HType)

	for currentOffset < entryEnd {
		subHeader, _, err := a.readHeader(currentOffset)
		if err != nil {
			return nil, fmt.Errorf("read sub-header at %d: %w", currentOffset, err)
		}

		switch subHeader.HType {
		case PXARXAttr:
			xattr, err := a.readXAttr(currentOffset + PxarHeaderSize)
			if err != nil {
				return nil, fmt.Errorf("read xattr: %w", err)
			}
			entry.metadata.XAttrs = append(entry.metadata.XAttrs, *xattr)

		case PXARFCaps:
			fcaps, err := a.readFCaps(currentOffset+PxarHeaderSize, subHeader.ContentSize())
			if err != nil {
				return nil, fmt.Errorf("read fcaps: %w", err)
			}
			entry.metadata.FCaps = fcaps

		case PXARAclUser:
			acl, err := a.readAclUser(currentOffset + PxarHeaderSize)
			if err != nil {
				return nil, fmt.Errorf("read acl user: %w", err)
			}
			entry.metadata.ACLUser = append(entry.metadata.ACLUser, *acl)

		case PXARAclGroup:
			acl, err := a.readAclGroup(currentOffset + PxarHeaderSize)
			if err != nil {
				return nil, fmt.Errorf("read acl group: %w", err)
			}
			entry.metadata.ACLGrp = append(entry.metadata.ACLGrp, *acl)

		case PXARPayload:
			contentStart := currentOffset + PxarHeaderSize
			contentEnd := currentOffset + subHeader.FullSize
			entry.entryRange.ContentInfo = &ContentInfo{
				Start: contentStart,
				End:   contentEnd,
			}
			currentOffset += subHeader.FullSize
			continue

		case PXARPayloadRef:
			payloadRef, err := a.readPayloadRef(currentOffset + PxarHeaderSize)
			if err != nil {
				return nil, fmt.Errorf("read payload ref: %w", err)
			}
			entry.entryRange.ContentInfo = &ContentInfo{
				Start: payloadRef.Offset,
				End:   payloadRef.Offset + payloadRef.Size,
			}

		case PXARSymlink, PXARHardlink, PXARDevice, PXARFilename, PXARGoodbye:
			currentOffset += subHeader.FullSize
			continue
		}

		currentOffset += subHeader.FullSize
	}

	ft := stat.FileType()
	switch {
	case ft == ModeIFDIR:
		entry.kind = EntryKindDirectory
	case ft == ModeIFLNK:
		entry.kind = EntryKindSymlink
	case ft == ModeIFREG:
		entry.kind = EntryKindFile
	case ft == ModeIFCHR || ft == ModeIFBLK:
		entry.kind = EntryKindDevice
	case ft == ModeIFIFO:
		entry.kind = EntryKindFifo
	case ft == ModeIFSOCK:
		entry.kind = EntryKindSocket
	}

	a.mu.Lock()
	a.entries[offset] = entry
	a.mu.Unlock()

	return entry, nil
}

func (a *Accessor) statSize(htype uint64) uint64 {
	if htype == PXAREntryV1 {
		return 28
	}
	return 36
}

func (a *Accessor) readStat(offset uint64, htype uint64) (*Stat, error) {
	statBytes := make([]byte, a.statSize(htype))
	n, err := a.metaReader.ReadAt(statBytes, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("read stat: %w", err)
	}
	if n != len(statBytes) {
		return nil, fmt.Errorf("incomplete stat read")
	}

	stat := &Stat{
		Mode:  binary.LittleEndian.Uint64(statBytes[0:8]),
		Flags: binary.LittleEndian.Uint64(statBytes[8:16]),
		UID:   binary.LittleEndian.Uint32(statBytes[16:20]),
		GID:   binary.LittleEndian.Uint32(statBytes[20:24]),
	}

	if htype == PXAREntryV1 {
		mtime := binary.LittleEndian.Uint64(statBytes[24:32])
		stat.Mtime = StatxTimestamp{
			Secs:  int64(mtime / 1e9),
			Nanos: uint32(mtime % 1e9),
		}
	} else {
		stat.Mtime = StatxTimestamp{
			Secs:  int64(binary.LittleEndian.Uint64(statBytes[24:32])),
			Nanos: binary.LittleEndian.Uint32(statBytes[32:36]),
		}
	}

	return stat, nil
}

func (a *Accessor) readXAttr(offset uint64) (*XAttr, error) {
	lenByte := make([]byte, 4)
	n, err := a.metaReader.ReadAt(lenByte, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("read xattr length: %w", err)
	}
	if n != 4 {
		return nil, fmt.Errorf("incomplete xattr length read")
	}

	totalLen := binary.LittleEndian.Uint32(lenByte)
	data := make([]byte, totalLen)
	n, err = a.metaReader.ReadAt(data, int64(offset+4))
	if err != nil {
		return nil, fmt.Errorf("read xattr data: %w", err)
	}
	if uint32(n) != totalLen {
		return nil, fmt.Errorf("incomplete xattr data read")
	}

	nullPos := bytes.IndexByte(data, 0)
	if nullPos < 0 {
		return nil, fmt.Errorf("invalid xattr format: no null separator")
	}

	return &XAttr{
		Data:    append([]byte{0}, append(data[:nullPos], append([]byte{0}, data[nullPos+1:]...)...)...),
		NameLen: nullPos,
	}, nil
}

func (a *Accessor) readFCaps(offset uint64, size uint64) ([]byte, error) {
	data := make([]byte, size)
	n, err := a.metaReader.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if uint64(n) != size {
		return nil, fmt.Errorf("incomplete fcaps read")
	}
	return data, nil
}

func (a *Accessor) readAclUser(offset uint64) (*ACLUserEntry, error) {
	data := make([]byte, 12)
	n, err := a.metaReader.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != 12 {
		return nil, fmt.Errorf("incomplete acl user read")
	}
	return &ACLUserEntry{
		UID:  binary.LittleEndian.Uint32(data[0:4]),
		Perm: binary.LittleEndian.Uint64(data[4:12]),
	}, nil
}

func (a *Accessor) readAclGroup(offset uint64) (*ACLGroupEntry, error) {
	data := make([]byte, 12)
	n, err := a.metaReader.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != 12 {
		return nil, fmt.Errorf("incomplete acl group read")
	}
	return &ACLGroupEntry{
		GID:  binary.LittleEndian.Uint32(data[0:4]),
		Perm: binary.LittleEndian.Uint64(data[4:12]),
	}, nil
}

func (a *Accessor) readPayloadRef(offset uint64) (*PayloadRef, error) {
	data := make([]byte, 16)
	n, err := a.metaReader.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != 16 {
		return nil, fmt.Errorf("incomplete payload ref read")
	}
	return &PayloadRef{
		Offset: binary.LittleEndian.Uint64(data[0:8]),
		Size:   binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}

func (a *Accessor) Root() *FileEntry {
	return a.root
}

func (a *Accessor) OpenRoot() (*FileEntry, error) {
	if a.root == nil {
		return a.openRoot()
	}
	return a.root, nil
}

func (a *Accessor) Lookup(pathStr string) (*FileEntry, error) {
	if pathStr == "" || pathStr == "/" {
		return a.OpenRoot()
	}

	components := splitPath(pathStr)
	current, err := a.OpenRoot()
	if err != nil {
		return nil, err
	}

	for _, comp := range components {
		if comp == "" {
			continue
		}

		if current.kind != EntryKindDirectory {
			return nil, ErrNotDirectory
		}

		entries, err := a.ReadDir(current.entryRange.EntryEnd)
		if err != nil {
			return nil, fmt.Errorf("readdir: %w", err)
		}

		found := false
		for _, entry := range entries {
			if entry.name == comp {
				current = &entry
				found = true
				break
			}
		}

		if !found {
			return nil, ErrNotFound
		}
	}

	return current, nil
}

func (a *Accessor) ReadDir(dirEnd uint64) ([]FileEntry, error) {
	if cached, ok := a.dirCache.Load(dirEnd); ok {
		return cached, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if cached, ok := a.dirCache.Load(dirEnd); ok {
		return cached, nil
	}

	goodbyeHeader, _, err := a.readHeader(dirEnd - PxarHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("read goodbye header: %w", err)
	}

	if goodbyeHeader.HType != PXARGoodbye {
		return nil, fmt.Errorf("not a goodbye table at %d", dirEnd-PxarHeaderSize)
	}

	goodbyeSize := goodbyeHeader.FullSize
	goodbyeStart := dirEnd - goodbyeSize
	numItems := (goodbyeSize - PxarHeaderSize) / 24

	goodbyeData := make([]byte, numItems*24)
	n, err := a.metaReader.ReadAt(goodbyeData, int64(goodbyeStart+PxarHeaderSize))
	if err != nil {
		return nil, fmt.Errorf("read goodbye table: %w", err)
	}
	if uint64(n) != numItems*24 {
		return nil, fmt.Errorf("incomplete goodbye table read")
	}

	entries := make([]FileEntry, 0, numItems)

	for i := uint64(0); i < numItems; i++ {
		itemOffset := i * 24
		hash := binary.LittleEndian.Uint64(goodbyeData[itemOffset : itemOffset+8])
		offset := binary.LittleEndian.Uint64(goodbyeData[itemOffset+8 : itemOffset+16])
		size := binary.LittleEndian.Uint64(goodbyeData[itemOffset+16 : itemOffset+24])

		if hash == PXARGoodbyeTail {
			continue
		}

		entryOffset := goodbyeStart - offset
		entry, err := a.readEntry(entryOffset)
		if err != nil {
			continue
		}

		filename, err := a.readFilename(entryOffset)
		if err != nil {
			continue
		}

		entry.name = filename
		entry.entryRange.EntryEnd = entryOffset + size
		entries = append(entries, *entry)
	}

	a.dirCache.Store(dirEnd, entries)
	return entries, nil
}

func (a *Accessor) readFilename(entryOffset uint64) (string, error) {
	filenameHeader, _, err := a.readHeader(entryOffset - PxarHeaderSize)
	if err != nil {
		return "", err
	}

	if filenameHeader.HType != PXARFilename {
		return "", fmt.Errorf("not a filename header")
	}

	filenameSize := filenameHeader.ContentSize()
	filenameBytes := make([]byte, filenameSize)
	n, err := a.metaReader.ReadAt(filenameBytes, int64(entryOffset))
	if err != nil {
		return "", err
	}
	if uint64(n) != filenameSize {
		return "", fmt.Errorf("incomplete filename read")
	}

	if len(filenameBytes) > 0 && filenameBytes[len(filenameBytes)-1] == 0 {
		filenameBytes = filenameBytes[:len(filenameBytes)-1]
	}

	return string(filenameBytes), nil
}

func (a *Accessor) OpenFileAtRange(rangeInfo *EntryRangeInfo) (*FileEntry, error) {
	return a.readEntry(rangeInfo.EntryStart)
}

func (a *Accessor) OpenDirAtEnd(end uint64) (*FileEntry, error) {
	entries, err := a.ReadDir(end)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrNotFound
	}
	return &entries[0], nil
}

func (e *FileEntry) Name() string {
	return e.name
}

func (e *FileEntry) Path() string {
	return e.path
}

func (e *FileEntry) Kind() EntryKind {
	return e.kind
}

func (e *FileEntry) Metadata() *Metadata {
	return e.metadata
}

func (e *FileEntry) EntryRange() EntryRangeInfo {
	return e.entryRange
}

func (e *FileEntry) IsDir() bool {
	return e.kind == EntryKindDirectory
}

func (e *FileEntry) IsFile() bool {
	return e.kind == EntryKindFile
}

func (e *FileEntry) IsSymlink() bool {
	return e.kind == EntryKindSymlink
}

func (e *FileEntry) FileSize() uint64 {
	if e.entryRange.ContentInfo != nil {
		return e.entryRange.ContentInfo.End - e.entryRange.ContentInfo.Start
	}
	return 0
}

func (e *FileEntry) Contents() (*FileContents, error) {
	if e.kind != EntryKindFile {
		return nil, fmt.Errorf("not a file")
	}

	if e.entryRange.ContentInfo == nil {
		return nil, fmt.Errorf("no content info")
	}

	return &FileContents{
		accessor: e.accessor,
		start:    e.entryRange.ContentInfo.Start,
		end:      e.entryRange.ContentInfo.End,
	}, nil
}

func (e *FileEntry) GetSymlink() (string, error) {
	if e.kind != EntryKindSymlink {
		return "", fmt.Errorf("not a symlink")
	}

	currentOffset := e.entryRange.EntryStart + PxarHeaderSize

	for currentOffset < e.entryRange.EntryEnd {
		subHeader, _, err := e.accessor.readHeader(currentOffset)
		if err != nil {
			return "", err
		}

		if subHeader.HType == PXARSymlink {
			targetBytes := make([]byte, subHeader.ContentSize())
			n, err := e.accessor.metaReader.ReadAt(targetBytes, int64(currentOffset+PxarHeaderSize))
			if err != nil {
				return "", err
			}
			if uint64(n) != subHeader.ContentSize() {
				return "", fmt.Errorf("incomplete symlink read")
			}

			if len(targetBytes) > 0 && targetBytes[len(targetBytes)-1] == 0 {
				targetBytes = targetBytes[:len(targetBytes)-1]
			}
			return string(targetBytes), nil
		}

		currentOffset += subHeader.FullSize
	}

	return "", fmt.Errorf("symlink target not found")
}

func (e *FileEntry) LookupSelf() (*FileEntry, error) {
	return e, nil
}

func (e *FileEntry) Lookup(childPath string) (*FileEntry, error) {
	return e.accessor.Lookup(path.Join(e.path, childPath))
}

func (e *FileEntry) ReadDir() ([]FileEntry, error) {
	if e.kind != EntryKindDirectory {
		return nil, ErrNotDirectory
	}
	return e.accessor.ReadDir(e.entryRange.EntryEnd)
}

type FileContents struct {
	accessor *Accessor
	start    uint64
	end      uint64
	offset   uint64
}

func (c *FileContents) ReadAt(p []byte, off int64) (int, error) {
	offset := uint64(off)
	if offset >= c.end-c.start {
		return 0, io.EOF
	}

	readEnd := offset + uint64(len(p))
	if readEnd > c.end-c.start {
		readEnd = c.end - c.start
		p = p[:readEnd-offset]
	}

	actualOffset := c.start + offset
	n, err := c.accessor.payloadReader.ReadAt(p, int64(actualOffset))
	if err != nil && err != io.EOF {
		return n, err
	}

	return n, nil
}

func (c *FileContents) Read(p []byte) (int, error) {
	n, err := c.ReadAt(p, int64(c.offset))
	c.offset += uint64(n)
	return n, err
}

func (c *FileContents) Seek(offset int64, whence int) (int64, error) {
	size := c.end - c.start
	switch whence {
	case io.SeekStart:
		c.offset = uint64(offset)
	case io.SeekCurrent:
		c.offset = uint64(int64(c.offset) + offset)
	case io.SeekEnd:
		c.offset = uint64(int64(size) + offset)
	}

	if c.offset > size {
		c.offset = size
	}

	return int64(c.offset), nil
}

func (c *FileContents) Size() uint64 {
	return c.end - c.start
}

func splitPath(p string) []string {
	if p == "" || p == "/" {
		return nil
	}

	p = path.Clean(p)
	var parts []string
	for {
		dir, file := path.Split(p)
		if file == "" {
			break
		}
		parts = append([]string{file}, parts...)
		p = path.Clean(dir)
		if p == "/" || p == "" {
			break
		}
	}
	return parts
}

func HashFilename(name []byte) uint64 {
	h := NewSipHash24(PXARHashKey1, PXARHashKey2)
	h.Write(name)
	return h.Sum64()
}

type SipHash24 struct {
	k0, k1         uint64
	v0, v1, v2, v3 uint64
	buf            []byte
}

func NewSipHash24(k0, k1 uint64) *SipHash24 {
	h := &SipHash24{k0: k0, k1: k1, buf: make([]byte, 0, 8)}
	h.v0 = k0 ^ 0x736f6d6570736575
	h.v1 = k1 ^ 0x646f72616e646f6d
	h.v2 = k0 ^ 0x6c7967656e657261
	h.v3 = k1 ^ 0x7465646279746573
	return h
}

func (h *SipHash24) Write(p []byte) (int, error) {
	h.buf = append(h.buf, p...)
	return len(p), nil
}

func (h *SipHash24) Reset() {
	h.buf = h.buf[:0]
	h.v0 = h.k0 ^ 0x736f6d6570736575
	h.v1 = h.k1 ^ 0x646f72616e646f6d
	h.v2 = h.k0 ^ 0x6c7967656e657261
	h.v3 = h.k1 ^ 0x7465646279746573
}

func (h *SipHash24) Sum64() uint64 {
	buf := h.buf
	length := uint64(len(buf)) << 56

	for len(buf) >= 8 {
		m := binary.LittleEndian.Uint64(buf[:8])
		h.v3 ^= m
		h.sipRound()
		h.sipRound()
		h.v0 ^= m
		buf = buf[8:]
	}

	var m uint64
	for i, b := range buf {
		m |= uint64(b) << (8 * uint(i))
	}

	h.v3 ^= m | length
	h.sipRound()
	h.sipRound()
	h.v0 ^= m | length
	h.v2 ^= 0xff
	h.sipRound()
	h.sipRound()
	h.sipRound()
	h.sipRound()
	return h.v0 ^ h.v1 ^ h.v2 ^ h.v3
}

func (h *SipHash24) sipRound() {
	h.v0 += h.v1
	h.v1 = (h.v1 << 13) | (h.v1 >> 51)
	h.v1 ^= h.v0
	h.v0 = (h.v0 << 32) | (h.v0 >> 32)
	h.v2 += h.v3
	h.v3 = (h.v3 << 16) | (h.v3 >> 48)
	h.v3 ^= h.v2
	h.v0 += h.v3
	h.v3 = (h.v3 << 21) | (h.v3 >> 43)
	h.v3 ^= h.v0
	h.v2 += h.v1
	h.v1 = (h.v1 << 17) | (h.v1 >> 47)
	h.v1 ^= h.v2
	h.v2 = (h.v2 << 32) | (h.v2 >> 32)
}

var _ io.Reader = (*FileContents)(nil)
var _ io.ReaderAt = (*FileContents)(nil)
var _ io.Seeker = (*FileContents)(nil)

func (a *Accessor) FollowHardlink(entry *FileEntry) (*FileEntry, error) {
	if entry.kind != EntryKindHardlink {
		return entry, nil
	}

	currentOffset := entry.entryRange.EntryStart + PxarHeaderSize

	for currentOffset < entry.entryRange.EntryEnd {
		subHeader, _, err := a.readHeader(currentOffset)
		if err != nil {
			return nil, err
		}

		if subHeader.HType == PXARHardlink {
			data := make([]byte, subHeader.ContentSize())
			n, err := a.metaReader.ReadAt(data, int64(currentOffset+PxarHeaderSize))
			if err != nil {
				return nil, err
			}
			if uint64(n) != subHeader.ContentSize() {
				return nil, fmt.Errorf("incomplete hardlink read")
			}

			targetOffset := binary.LittleEndian.Uint64(data[len(data)-8:])

			targetEntry, err := a.readEntry(targetOffset)
			if err != nil {
				return nil, err
			}

			if targetEntry.kind == EntryKindHardlink {
				return nil, fmt.Errorf("hardlink cycle detected")
			}

			return targetEntry, nil
		}

		currentOffset += subHeader.FullSize
	}

	return nil, fmt.Errorf("hardlink target not found")
}

func (e *FileEntry) IntoMetadata() *Metadata {
	return e.metadata
}

func (m *Metadata) Xattrs() []XAttr {
	return m.XAttrs
}

func ReadAtAll(r Reader, p []byte, offset int64) error {
	n, err := r.ReadAt(p, offset)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (a *Accessor) MetaSize() uint64 {
	return a.metaSize
}

func (a *Accessor) PayloadSize() uint64 {
	return a.payloadSize
}

func (a *Accessor) ReadAt(p []byte, offset int64) (int, error) {
	if uint64(offset) < a.metaSize {
		return a.metaReader.ReadAt(p, offset)
	}
	return a.payloadReader.ReadAt(p, int64(uint64(offset)-a.metaSize))
}
