package pxarmount

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"golang.org/x/sys/unix"
)

func addDataExtent(extents []dataExtent, start, end uint64) []dataExtent {
	return mergeDataExtents(extents, []dataExtent{{Start: start, End: end}})
}

// insertDataExtent coalesces on insert; sequential writes hit the O(1) tail case.
func insertDataExtent(extents []dataExtent, start, end uint64) []dataExtent {
	if start >= end {
		return extents
	}
	if n := len(extents); n > 0 {
		if last := &extents[n-1]; start >= last.Start && start <= last.End {
			if end > last.End {
				last.End = end
			}
			return extents
		}
	}
	i, _ := slices.BinarySearchFunc(extents, start, func(e dataExtent, target uint64) int {
		return cmp.Compare(e.Start, target)
	})
	if i > 0 && extents[i-1].End >= start {
		i--
		if end < extents[i].End {
			end = extents[i].End
		}
		start = extents[i].Start
	} else {
		extents = slices.Insert(extents, i, dataExtent{})
	}
	j := i + 1
	for j < len(extents) && extents[j].Start <= end {
		if extents[j].End > end {
			end = extents[j].End
		}
		j++
	}
	extents[i] = dataExtent{Start: start, End: end}
	if j > i+1 {
		extents = slices.Delete(extents, i+1, j)
	}
	return extents
}

func alignDown(v, blockSize uint64) uint64 { return v / blockSize * blockSize }
func alignUp(v, blockSize uint64) uint64 {
	if v == 0 {
		return 0
	}
	return (v-1)/blockSize*blockSize + blockSize
}

// blockIsHole reports whether the block at blockStart is entirely unallocated.
// An allocated block is already fully valid by induction, so it needs no fill.
func blockIsHole(fd int, blockStart, blockSize uint64) (bool, error) {
	off, err := unix.Seek(fd, int64(blockStart), unix.SEEK_DATA)
	if err == unix.ENXIO {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return uint64(off) >= blockStart+blockSize, nil
}

// fillSparseMargins materialises the lower-layer bytes that share a block with
// a partial write, so the block becomes wholly backed by the overlay. Without
// it, crash recovery from the allocation map would claim never-written bytes
// in the same block and shadow the backup content with zeroes.
func (fs *MutableFS) fillSparseMargins(fh *passFh, off, end uint64) (uint64, error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(fh.fd, &stat); err != nil {
		return 0, err
	}
	blockSize := uint64(stat.Blksize)
	if blockSize == 0 {
		return 0, syscall.EIO
	}
	if fh.pxarNode == nil || fh.lowerSize == 0 {
		return blockSize, nil
	}

	margins := [2][2]uint64{
		{alignDown(off, blockSize), off},
		{end, alignUp(end, blockSize)},
	}

	var holes [2]bool
	for i, m := range margins {
		if m[0] >= min(m[1], fh.lowerSize) {
			continue
		}
		hole, err := blockIsHole(fh.fd, alignDown(m[0], blockSize), blockSize)
		if err != nil {
			return 0, err
		}
		holes[i] = hole
	}

	for i, m := range margins {
		if !holes[i] {
			continue
		}
		start, stop := m[0], min(m[1], fh.lowerSize)
		if start >= stop {
			continue
		}
		buf := make([]byte, stop-start)
		n, err := fs.pxar.readFileAt(fh.pxarNode, int64(start), buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			continue
		}
		if _, err := syscall.Pwrite(fh.fd, buf[:n], int64(start)); err != nil {
			return 0, err
		}
	}
	return blockSize, nil
}

// rebuildDataExtents recovers extents from the file's allocation map after a
// crash; without it, written regions read back as original backup content.
func rebuildDataExtents(absPath string, size uint64) ([]dataExtent, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	fd := int(f.Fd())
	var extents []dataExtent
	var off int64
	for uint64(off) < size {
		dataStart, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if err == unix.ENXIO {
				break
			}
			return nil, err
		}
		holeStart, err := unix.Seek(fd, dataStart, unix.SEEK_HOLE)
		if err != nil {
			return nil, err
		}
		if uint64(holeStart) > size {
			holeStart = int64(size)
		}
		if dataStart < holeStart {
			extents = append(extents, dataExtent{Start: uint64(dataStart), End: uint64(holeStart)})
		}
		if holeStart <= off {
			break
		}
		off = holeStart
	}
	return extents, nil
}

func mergeDataExtents(existing, pending []dataExtent) []dataExtent {
	extents := make([]dataExtent, 0, len(existing)+len(pending))
	extents = append(extents, existing...)
	extents = append(extents, pending...)
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].Start < extents[j].Start
	})
	merged := extents[:0]
	for _, extent := range extents {
		if extent.Start >= extent.End {
			continue
		}
		if len(merged) == 0 || merged[len(merged)-1].End < extent.Start {
			merged = append(merged, extent)
			continue
		}
		merged[len(merged)-1].End = max(merged[len(merged)-1].End, extent.End)
	}
	return merged
}

func trimDataExtents(extents []dataExtent, size uint64) []dataExtent {
	trimmed := make([]dataExtent, 0, len(extents))
	for _, extent := range extents {
		if extent.Start >= size {
			break
		}
		extent.End = min(extent.End, size)
		if extent.Start < extent.End {
			trimmed = append(trimmed, extent)
		}
	}
	return trimmed
}

func (fs *MutableFS) sparseDataExtents(inode uint64, node *GraphNode) []dataExtent {
	if meta, ok := fs.dirtyMeta.Load(inode); ok && meta.dataExtents != nil {
		return mergeDataExtents(node.DataExtents, meta.dataExtents)
	}
	return node.DataExtents
}

func (fs *MutableFS) flushDirtyMeta(inode uint64) error {
	inoMu := fs.getInoLock(inode)
	inoMu.Lock()
	defer inoMu.Unlock()
	meta, ok := fs.dirtyMeta.LoadAndDelete(inode)
	if !ok {
		return nil
	}
	path := fs.inodeToPath(inode)
	if path == "" {
		return meta.writeErr
	}
	resolved, status := fs.resolve(path)
	if status != fuse.OK || resolved.Node == nil {
		return fmt.Errorf("resolve dirty file %q: %s", path, status)
	}
	if meta.size > resolved.Node.Size {
		resolved.Node.Size = meta.size
	}
	resolved.Node.MtimeNs = meta.mtimeNs
	resolved.Node.CtimeNs = meta.ctimeNs
	if resolved.Node.SparseData && meta.dataExtents != nil {
		resolved.Node.DataExtents = mergeDataExtents(resolved.Node.DataExtents, meta.dataExtents)
	}
	if err := fs.journal.UpdateNode(resolved.Node); err != nil {
		return err
	}
	return meta.writeErr
}

func (fs *MutableFS) flushAllDirtyMeta() error {
	var firstErr error
	fs.dirtyMeta.Range(func(inode uint64, _ pendingMeta) bool {
		if err := fs.flushDirtyMeta(inode); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	return firstErr
}

func (fs *MutableFS) readSparseAt(fd int, inode uint64, graphNode *GraphNode, lowerNode *node, dest []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return 0, err
	}
	if offset >= st.Size {
		return 0, io.EOF
	}
	if remaining := st.Size - offset; int64(len(dest)) > remaining {
		dest = dest[:remaining]
	}
	extents := fs.sparseDataExtents(inode, graphNode)
	start := uint64(offset)
	end := start + uint64(len(dest))
	position := start
	for _, extent := range extents {
		if extent.End <= position {
			continue
		}
		if extent.Start >= end {
			break
		}
		if position < extent.Start {
			holeEnd := min(extent.Start, end)
			if err := fs.readLowerRange(graphNode, lowerNode, dest[position-start:holeEnd-start], position); err != nil {
				return int(position - start), err
			}
			position = holeEnd
		}
		if position >= end {
			break
		}
		dataEnd := min(extent.End, end)
		n, err := syscall.Pread(fd, dest[position-start:dataEnd-start], int64(position))
		position += uint64(n)
		if err != nil {
			return int(position - start), err
		}
		if position != dataEnd {
			return int(position - start), io.ErrUnexpectedEOF
		}
	}
	if position < end {
		if err := fs.readLowerRange(graphNode, lowerNode, dest[position-start:], position); err != nil {
			return int(position - start), err
		}
		position = end
	}
	return int(position - start), nil
}

func (fs *MutableFS) readLowerRange(graphNode *GraphNode, lowerNode *node, dest []byte, offset uint64) error {
	clear(dest)
	if offset >= graphNode.LowerSize {
		return nil
	}
	length := min(uint64(len(dest)), graphNode.LowerSize-offset)
	if lowerNode == nil {
		return fmt.Errorf("sparse lower file is unavailable")
	}
	n, err := fs.pxar.readFileAt(lowerNode, int64(offset), dest[:length])
	if uint64(n) != length {
		return io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	return nil
}

type sparseBackedReader struct {
	reader *io.SectionReader
	file   *os.File
}

func (r *sparseBackedReader) Read(dest []byte) (int, error) {
	return r.reader.Read(dest)
}

func (r *sparseBackedReader) Close() error {
	return r.file.Close()
}

type sparseReaderAt struct {
	fs        *MutableFS
	fd        int
	inode     uint64
	graphNode *GraphNode
	lowerNode *node
}

func (r *sparseReaderAt) ReadAt(dest []byte, offset int64) (int, error) {
	requested := len(dest)
	n, err := r.fs.readSparseAt(r.fd, r.inode, r.graphNode, r.lowerNode, dest, offset)
	if err == nil && n < requested {
		err = io.EOF
	}
	return n, err
}

func (fs *MutableFS) openBackedFile(relPath string, graphNode *GraphNode) (io.ReadCloser, int64, error) {
	abs := fs.mutablePath(relPath)
	file, err := os.Open(abs)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	if graphNode == nil && fs.journal == nil {
		return file, info.Size(), nil
	}
	if graphNode == nil {
		resolved, status := fs.resolve(relPath)
		if status != fuse.OK || resolved.Node == nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("resolve backed file %q: %s", relPath, status)
		}
		graphNode = resolved.Node
	}
	if !graphNode.SparseData {
		return file, info.Size(), nil
	}
	lowerNode := fs.findPxarNode(graphNode.RedirectTo)
	inode := fs.pathToIno(relPath, false)
	readerAt := &sparseReaderAt{
		fs:        fs,
		fd:        int(file.Fd()),
		inode:     inode,
		graphNode: graphNode,
		lowerNode: lowerNode,
	}
	return &sparseBackedReader{
		reader: io.NewSectionReader(readerAt, 0, info.Size()),
		file:   file,
	}, info.Size(), nil
}
