package native

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type PxarEncoder struct {
	metaWriter    *ChunkWriter
	payloadWriter *ChunkWriter
	metaOffset    uint64
	payloadOffset uint64

	options EncoderOptions
}

type EncoderOptions struct {
	WithXAttrs     bool
	WithACLs       bool
	WithFCaps      bool
	WithPayloads   bool
	FollowSymlinks bool
}

func DefaultEncoderOptions() EncoderOptions {
	return EncoderOptions{
		WithXAttrs:     true,
		WithACLs:       true,
		WithFCaps:      true,
		WithPayloads:   true,
		FollowSymlinks: false,
	}
}

func NewPxarEncoder(metaStore, payloadStore *ChunkStoreWriter, metaIndex, payloadIndex *DynamicIndexWriter, options EncoderOptions) *PxarEncoder {
	return &PxarEncoder{
		metaWriter:    NewChunkWriter(metaStore, metaIndex, 4*1024*1024),
		payloadWriter: NewChunkWriter(payloadStore, payloadIndex, 4*1024*1024),
		options:       options,
	}
}

func (e *PxarEncoder) WriteFormatHeader() error {
	header := make([]byte, PxarHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], PXARFormatVersion)
	binary.LittleEndian.PutUint64(header[8:16], PxarHeaderSize)

	n, err := e.metaWriter.Write(header)
	if err != nil {
		return err
	}
	e.metaOffset += uint64(n)

	return nil
}

func (e *PxarEncoder) encodeStat(info os.FileInfo) ([]byte, error) {
	stat := info.Sys().(*syscall.Stat_t)
	if stat == nil {
		return nil, fmt.Errorf("cannot get stat info")
	}

	data := make([]byte, 36)

	mode := uint64(stat.Mode)
	binary.LittleEndian.PutUint64(data[0:8], mode)
	binary.LittleEndian.PutUint64(data[8:16], 0)
	binary.LittleEndian.PutUint32(data[16:20], stat.Uid)
	binary.LittleEndian.PutUint32(data[20:24], uint32(stat.Gid))

	mtime := info.ModTime()
	binary.LittleEndian.PutUint64(data[24:32], uint64(mtime.Unix()))
	binary.LittleEndian.PutUint32(data[32:36], uint32(mtime.Nanosecond()))

	return data, nil
}

func (e *PxarEncoder) writeHeader(htype uint64, contentSize uint64, w io.Writer) (int, error) {
	header := make([]byte, PxarHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], htype)
	binary.LittleEndian.PutUint64(header[8:16], PxarHeaderSize+contentSize)
	return w.Write(header)
}

func (e *PxarEncoder) EncodeFile(path string, info os.FileInfo, content []byte) error {
	entryStart := e.metaOffset

	statData, err := e.encodeStat(info)
	if err != nil {
		return fmt.Errorf("encode stat: %w", err)
	}

	headerBuf := &bufferAtWriter{buf: make([]byte, 0)}
	contentBuf := &bufferAtWriter{buf: make([]byte, 0)}

	if _, err := e.writeHeader(PXAREntry, uint64(len(statData)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(statData)

	name := filepath.Base(path)
	nameBytes := []byte(name)
	if len(nameBytes) > 0 && nameBytes[len(nameBytes)-1] != 0 {
		nameBytes = append(nameBytes, 0)
	}
	if _, err := e.writeHeader(PXARFilename, uint64(len(nameBytes)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(nameBytes)

	if e.options.WithXAttrs {
		xattrs, err := getXAttrs(path)
		if err == nil && len(xattrs) > 0 {
			for name, value := range xattrs {
				xattrData := make([]byte, 4+len(name)+1+len(value))
				binary.LittleEndian.PutUint32(xattrData[0:4], uint32(len(xattrData)-4))
				copy(xattrData[4:], name)
				xattrData[4+len(name)] = 0
				copy(xattrData[4+len(name)+1:], value)

				if _, err := e.writeHeader(PXARXAttr, uint64(len(xattrData)), headerBuf); err != nil {
					return err
				}
				headerBuf.Write(xattrData)
			}
		}
	}

	if len(content) > 0 {
		contentStart := e.payloadOffset

		n, err := e.payloadWriter.Write(content)
		if err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
		e.payloadOffset += uint64(n)

		payloadRef := make([]byte, 16)
		binary.LittleEndian.PutUint64(payloadRef[0:8], contentStart)
		binary.LittleEndian.PutUint64(payloadRef[8:16], uint64(len(content)))

		if _, err := e.writeHeader(PXARPayloadRef, 16, headerBuf); err != nil {
			return err
		}
		headerBuf.Write(payloadRef)

		contentBuf.Write(payloadRef)
	}

	fullEntry := headerBuf.Bytes()
	entryEnd := e.metaOffset + uint64(len(fullEntry))

	n, err := e.metaWriter.Write(fullEntry)
	if err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	e.metaOffset += uint64(n)

	_ = entryStart
	_ = entryEnd
	_ = contentBuf

	return nil
}

func (e *PxarEncoder) EncodeDirectory(path string, info os.FileInfo, entries []os.FileInfo) error {
	statData, err := e.encodeStat(info)
	if err != nil {
		return fmt.Errorf("encode stat: %w", err)
	}

	headerBuf := &bufferAtWriter{buf: make([]byte, 0)}

	if _, err := e.writeHeader(PXAREntry, uint64(len(statData)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(statData)

	name := filepath.Base(path)
	if name == "." || name == "" {
		name = "/"
	}
	nameBytes := []byte(name)
	if len(nameBytes) > 0 && nameBytes[len(nameBytes)-1] != 0 {
		nameBytes = append(nameBytes, 0)
	}
	if _, err := e.writeHeader(PXARFilename, uint64(len(nameBytes)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(nameBytes)

	dirStart := e.metaOffset + uint64(len(headerBuf.Bytes()))

	goodbyeItems := make([]goodbyeItem, 0, len(entries))

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		entryStart := e.metaOffset

		if entry.IsDir() {
			subEntries, err := os.ReadDir(entryPath)
			if err != nil {
				continue
			}

			subInfos := make([]os.FileInfo, 0, len(subEntries))
			for _, se := range subEntries {
				si, err := se.Info()
				if err != nil {
					continue
				}
				subInfos = append(subInfos, si)
			}

			if err := e.EncodeDirectory(entryPath, entry, subInfos); err != nil {
				return err
			}
		} else if entry.Mode()&os.ModeSymlink != 0 {
			if err := e.EncodeSymlink(entryPath, entry); err != nil {
				return err
			}
		} else {
			content, err := os.ReadFile(entryPath)
			if err != nil {
				continue
			}
			if err := e.EncodeFile(entryPath, entry, content); err != nil {
				return err
			}
		}

		entryEnd := e.metaOffset
		entrySize := entryEnd - entryStart

		nameBytes := []byte(entry.Name())
		hash := HashFilename(nameBytes)

		goodbyeItems = append(goodbyeItems, goodbyeItem{
			hash:   hash,
			offset: dirStart - entryStart,
			size:   entrySize,
		})
	}

	for _, item := range goodbyeItems {
		goodbyeEntry := make([]byte, 24)
		binary.LittleEndian.PutUint64(goodbyeEntry[0:8], item.hash)
		binary.LittleEndian.PutUint64(goodbyeEntry[8:16], item.offset)
		binary.LittleEndian.PutUint64(goodbyeEntry[16:24], item.size)

		if _, err := e.writeHeader(PXARGoodbye, 24, headerBuf); err != nil {
			return err
		}
		headerBuf.Write(goodbyeEntry)
	}

	tailEntry := make([]byte, 24)
	binary.LittleEndian.PutUint64(tailEntry[0:8], PXARGoodbyeTail)
	binary.LittleEndian.PutUint64(tailEntry[8:16], 0)
	binary.LittleEndian.PutUint64(tailEntry[16:24], 0)

	if _, err := e.writeHeader(PXARGoodbye, 24, headerBuf); err != nil {
		return err
	}
	headerBuf.Write(tailEntry)

	n, err := e.metaWriter.Write(headerBuf.Bytes())
	if err != nil {
		return fmt.Errorf("write directory: %w", err)
	}
	e.metaOffset += uint64(n)

	return nil
}

type goodbyeItem struct {
	hash   uint64
	offset uint64
	size   uint64
}

func (e *PxarEncoder) EncodeSymlink(path string, info os.FileInfo) error {
	statData, err := e.encodeStat(info)
	if err != nil {
		return fmt.Errorf("encode stat: %w", err)
	}

	headerBuf := &bufferAtWriter{buf: make([]byte, 0)}

	if _, err := e.writeHeader(PXAREntry, uint64(len(statData)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(statData)

	name := filepath.Base(path)
	nameBytes := []byte(name)
	if len(nameBytes) > 0 && nameBytes[len(nameBytes)-1] != 0 {
		nameBytes = append(nameBytes, 0)
	}
	if _, err := e.writeHeader(PXARFilename, uint64(len(nameBytes)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(nameBytes)

	target, err := os.Readlink(path)
	if err != nil {
		return fmt.Errorf("read symlink: %w", err)
	}
	targetBytes := []byte(target)
	if len(targetBytes) > 0 && targetBytes[len(targetBytes)-1] != 0 {
		targetBytes = append(targetBytes, 0)
	}

	if _, err := e.writeHeader(PXARSymlink, uint64(len(targetBytes)), headerBuf); err != nil {
		return err
	}
	headerBuf.Write(targetBytes)

	n, err := e.metaWriter.Write(headerBuf.Bytes())
	if err != nil {
		return fmt.Errorf("write symlink: %w", err)
	}
	e.metaOffset += uint64(n)

	return nil
}

func (e *PxarEncoder) EncodeRoot(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat root: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("root must be a directory")
	}

	if err := e.WriteFormatHeader(); err != nil {
		return fmt.Errorf("write format header: %w", err)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read root directory: %w", err)
	}

	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		infos = append(infos, info)
	}

	return e.EncodeDirectory(path, info, infos)
}

func (e *PxarEncoder) Close() error {
	if e.metaWriter != nil {
		if err := e.metaWriter.Flush(); err != nil {
			return err
		}
	}
	if e.payloadWriter != nil {
		if err := e.payloadWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}

type bufferAtWriter struct {
	buf []byte
}

func (b *bufferAtWriter) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *bufferAtWriter) Bytes() []byte {
	return b.buf
}

func getXAttrs(path string) (map[string][]byte, error) {
	xattrs := make(map[string][]byte)

	return xattrs, nil
}

type PxarCreator struct {
	metaStore    *ChunkStoreWriter
	payloadStore *ChunkStoreWriter
	metaIndex    *DynamicIndexWriter
	payloadIndex *DynamicIndexWriter
	encoder      *PxarEncoder

	rootPath string
	options  EncoderOptions
}

func NewPxarCreator(rootPath, chunkStorePath string, options EncoderOptions) (*PxarCreator, error) {
	var crypt *CryptConfig
	compress := true

	metaStore := NewChunkStoreWriter(chunkStorePath, crypt, compress)
	payloadStore := NewChunkStoreWriter(chunkStorePath, crypt, compress)

	timestamp := time.Now().Unix()

	metaIndexPath := filepath.Join(chunkStorePath, fmt.Sprintf("mpxar-%d.didx", timestamp))
	payloadIndexPath := filepath.Join(chunkStorePath, fmt.Sprintf("ppxar-%d.didx", timestamp))

	metaIndex, err := NewDynamicIndexWriter(metaIndexPath)
	if err != nil {
		return nil, fmt.Errorf("create meta index: %w", err)
	}

	payloadIndex, err := NewDynamicIndexWriter(payloadIndexPath)
	if err != nil {
		metaIndex.Close()
		return nil, fmt.Errorf("create payload index: %w", err)
	}

	encoder := NewPxarEncoder(metaStore, payloadStore, metaIndex, payloadIndex, options)

	return &PxarCreator{
		metaStore:    metaStore,
		payloadStore: payloadStore,
		metaIndex:    metaIndex,
		payloadIndex: payloadIndex,
		encoder:      encoder,
		rootPath:     rootPath,
		options:      options,
	}, nil
}

func (c *PxarCreator) Create() error {
	return c.encoder.EncodeRoot(c.rootPath)
}

func (c *PxarCreator) Close() error {
	return c.encoder.Close()
}

func (c *PxarCreator) MetaIndexPath() string {
	return c.metaIndex.file.Name()
}

func (c *PxarCreator) PayloadIndexPath() string {
	return c.payloadIndex.file.Name()
}
