package pxarmount

import (
	"fmt"
	"io"
	"os"
	"sort"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func addDataExtent(extents []dataExtent, start, end uint64) []dataExtent {
	return mergeDataExtents(extents, []dataExtent{{Start: start, End: end}})
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
		return nil
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
	return fs.journal.UpdateNode(resolved.Node)
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
	if err != nil && err != io.EOF {
		return err
	}
	if uint64(n) != length {
		return io.ErrUnexpectedEOF
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
