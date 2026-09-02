//go:build linux

package arpcfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bodgit/sevenzip"
)

// m7z marks entries served by the sevenzip library instead of flate.
const m7z uint16 = 0x8000

var magic7z = [6]byte{'7', 'z', 0xbc, 0xaf, 0x27, 0x1c}

// parseArchiveOverlay sniffs the magic and dispatches to the matching parser.
func parseArchiveOverlay(readAt func(ctx context.Context, p []byte, off int64) (int, error), size, maxEntries int64) (*zipOverlay, error) {
	var magic [6]byte
	if _, err := readAt(context.Background(), magic[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if magic == magic7z {
		return parseSevenZipOverlay(readAt, size, maxEntries)
	}
	return parseZipOverlay(readAt, size, maxEntries)
}

// parseSevenZipOverlay expands a 7z into the shared overlay. Solid folders are
// fine: readdir emits pack order and the library pools folder decoders.
// ponytail: per-open LZMA dict is sized by archive properties; ulikunitz
// rejects absurd dicts, which demotes via the probe.
func parseSevenZipOverlay(readAt func(ctx context.Context, p []byte, off int64) (int, error), size, maxEntries int64) (*zipOverlay, error) {
	var hdr [32]byte
	if _, err := readAt(context.Background(), hdr[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if binary.LittleEndian.Uint64(hdr[20:]) == 0 {
		return emptyOverlay(readAt, size), nil
	}

	zr, err := sevenzip.NewReader(zipSrc{readAt}, size)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errZipUnsupported, err)
	}
	if int64(len(zr.File)) > maxEntries {
		return nil, fmt.Errorf("%w: %d entries exceeds %d", errZipTooMany, len(zr.File), maxEntries)
	}
	if err := sevenProbeFirst(zr); err != nil {
		return nil, err
	}

	ov := &zipOverlay{
		size:       size,
		entryCount: int64(len(zr.File)),
		readAt:     readAt,
		byName:     make(map[string]int32, len(zr.File)),
		dirs:       map[string]*zipDir{"": {}},
	}

	for _, f := range zr.File {
		name, namedDir, ok := cleanZipName(f.Name)
		if !ok {
			continue
		}
		m := f.Mode()
		isDir := namedDir || m.IsDir()
		var mtime int64
		if !f.Modified.IsZero() {
			mtime = f.Modified.Unix()
		}
		if isDir {
			if _, exists := ov.byName[name]; exists {
				continue
			}
			ov.ensureDir(name, uint32(m.Perm()), mtime)
			continue
		}
		mode := uint32(m.Perm())
		if mode == 0 {
			mode = 0o644
		}
		if m&os.ModeSymlink != 0 {
			mode |= uint32(os.ModeSymlink)
		}
		idx := int32(len(ov.entries))
		ov.entries = append(ov.entries, zipEntry{
			name:       name,
			method:     m7z,
			uncompSize: int64(f.UncompressedSize),
			dataOff:    -1,
			mode:       mode,
			mtime:      mtime,
			sidx:       int32(len(ov.sfiles)),
		})
		ov.sfiles = append(ov.sfiles, f)
		ov.byName[name] = idx
		ov.uncompSum += int64(f.UncompressedSize)
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

// sevenProbeFirst demotes encrypted/unsupported coders up front via a 1-byte read.
func sevenProbeFirst(zr *sevenzip.Reader) error {
	for _, f := range zr.File {
		if f.FileInfo().IsDir() || f.UncompressedSize == 0 {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("%w: %w", errZipUnsupported, err)
		}
		var b [1]byte
		_, rerr := io.ReadFull(rc, b[:])
		cerr := rc.Close()
		if rerr != nil || cerr != nil {
			return fmt.Errorf("%w: %w", errZipUnsupported, errors.Join(rerr, cerr))
		}
		return nil
	}
	return nil
}
