package native

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPxarFormatConstants(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected uint64
	}{
		{"PXARFormatVersion", PXARFormatVersion, 0x730f6c75df16a40d},
		{"PXAREntry", PXAREntry, 0xd5956474e588acef},
		{"PXAREntryV1", PXAREntryV1, 0x11da850a1c1cceff},
		{"PXARPrelude", PXARPrelude, 0xe309d79d9f7b771b},
		{"PXARFilename", PXARFilename, 0x16701121063917b3},
		{"PXARSymlink", PXARSymlink, 0x27f971e7dbf5dc5f},
		{"PXARDevice", PXARDevice, 0x9fc9e906586d5ce9},
		{"PXARXAttr", PXARXAttr, 0x0dab0229b57dcd03},
		{"PXARAclUser", PXARAclUser, 0x2ce8540a457d55b8},
		{"PXARAclGroup", PXARAclGroup, 0x136e3eceb04c03ab},
		{"PXARFCaps", PXARFCaps, 0x2da9dd9db5f7fb67},
		{"PXARHardlink", PXARHardlink, 0x51269c8422bd7275},
		{"PXARPayload", PXARPayload, 0x28147a1b0b7c1a25},
		{"PXARPayloadRef", PXARPayloadRef, 0x419d3d6bc4ba977e},
		{"PXARGoodbye", PXARGoodbye, 0x2fec4fa642d5731d},
		{"PXARGoodbyeTail", PXARGoodbyeTail, 0xef5eed5b753e1555},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.value, "constant mismatch for %s", tt.name)
		})
	}
}

func TestBlobMagicConstants(t *testing.T) {
	tests := []struct {
		name     string
		value    [8]byte
		expected [8]byte
	}{
		{"ProxmoxCatalogFileMagic10", ProxmoxCatalogFileMagic10, [8]byte{145, 253, 96, 249, 196, 103, 88, 213}},
		{"UncompressedBlobMagic10", UncompressedBlobMagic10, [8]byte{66, 171, 56, 7, 190, 131, 112, 161}},
		{"CompressedBlobMagic10", CompressedBlobMagic10, [8]byte{49, 185, 88, 66, 111, 182, 163, 127}},
		{"EncryptedBlobMagic10", EncryptedBlobMagic10, [8]byte{123, 103, 133, 190, 34, 45, 76, 240}},
		{"EncryptedCompressedBlobMagic10", EncryptedCompressedBlobMagic10, [8]byte{230, 89, 27, 191, 11, 191, 216, 11}},
		{"FixedSizedChunkIndex10", FixedSizedChunkIndex10, [8]byte{47, 127, 65, 237, 145, 253, 15, 205}},
		{"DynamicSizedChunkIndex10", DynamicSizedChunkIndex10, [8]byte{28, 145, 78, 165, 25, 186, 179, 205}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.value, "magic mismatch for %s", tt.name)
		})
	}
}

func TestHeaderParsing(t *testing.T) {
	header := &Header{
		HType:    PXAREntry,
		FullSize: 128,
	}

	assert.Equal(t, uint64(16), PxarHeaderSize)
	assert.Equal(t, uint64(128-16), header.ContentSize())

	header.FullSize = 8
	assert.Equal(t, uint64(0), header.ContentSize())
}

func TestDataBlobEncoding(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		compress bool
	}{
		{"small uncompressed", []byte("hello world"), false},
		{"small compressed", []byte("hello world"), true},
		{"larger uncompressed", bytes.Repeat([]byte("test data "), 1000), false},
		{"larger compressed", bytes.Repeat([]byte("test data "), 1000), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, err := EncodeDataBlob(tt.data, nil, tt.compress)
			require.NoError(t, err)
			require.NotNil(t, blob)

			decoded, err := blob.Decode(nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.data, decoded)

			err = blob.VerifyCRC()
			assert.NoError(t, err)
		})
	}
}

func TestDataBlobWithEncryption(t *testing.T) {
	key := [32]byte{}
	for i := range key {
		key[i] = byte(i)
	}

	crypt, err := NewCryptConfig(key)
	require.NoError(t, err)

	data := []byte("this is sensitive data that should be encrypted this is sensitive data that should be encrypted this is sensitive data that should be encrypted")

	tests := []struct {
		name     string
		compress bool
	}{
		{"encrypted uncompressed", false},
		{"encrypted compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, err := EncodeDataBlob(data, crypt, tt.compress)
			require.NoError(t, err)
			require.NotNil(t, blob)

			assert.True(t, blob.IsEncrypted())
			if tt.compress && len(data) > 64 {
				assert.True(t, blob.IsCompressed())
			}

			digest := Sha256Sum(data)
			decoded, err := blob.Decode(crypt, &digest)
			require.NoError(t, err)
			assert.Equal(t, data, decoded)
		})
	}
}

func TestSipHash24(t *testing.T) {
	h := NewSipHash24(PXARHashKey1, PXARHashKey2)

	testCases := []struct {
		input    string
		expected uint64
	}{
		{"test", 0},
		{"", 0},
		{"filename.txt", 0},
	}

	for _, tc := range testCases {
		h.Reset()
		h.Write([]byte(tc.input))
		hash := h.Sum64()
		assert.NotEqual(t, uint64(0), hash, "hash should not be zero for input: %s", tc.input)
	}

	h1 := NewSipHash24(PXARHashKey1, PXARHashKey2)
	h1.Write([]byte("hello"))
	hash1 := h1.Sum64()

	h2 := NewSipHash24(PXARHashKey1, PXARHashKey2)
	h2.Write([]byte("hello"))
	hash2 := h2.Sum64()

	assert.Equal(t, hash1, hash2, "same input should produce same hash")

	h3 := NewSipHash24(PXARHashKey1, PXARHashKey2)
	h3.Write([]byte("world"))
	hash3 := h3.Sum64()

	assert.NotEqual(t, hash1, hash3, "different input should produce different hash")
}

func TestSha256Sum(t *testing.T) {
	data := []byte("test data for hashing")
	digest := Sha256Sum(data)

	assert.Len(t, digest, 32, "SHA256 digest should be 32 bytes")

	data2 := []byte("test data for hashing")
	digest2 := Sha256Sum(data2)
	assert.Equal(t, digest, digest2, "same data should produce same digest")

	data3 := []byte("different data")
	digest3 := Sha256Sum(data3)
	assert.NotEqual(t, digest, digest3, "different data should produce different digest")
}

func TestDynamicIndexRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.didx")

	writer, err := NewDynamicIndexWriter(indexPath)
	require.NoError(t, err)

	digest1 := Sha256Sum([]byte("chunk1"))
	digest2 := Sha256Sum([]byte("chunk2"))
	digest3 := Sha256Sum([]byte("chunk3"))

	err = writer.AddChunk(1000, digest1)
	require.NoError(t, err)

	err = writer.AddChunk(2500, digest2)
	require.NoError(t, err)

	err = writer.AddChunk(4000, digest3)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := OpenDynamicIndex(indexPath)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, 3, reader.EntryCount())
	assert.Equal(t, uint64(4000), reader.ArchiveSize())
	assert.Equal(t, writer.UUID(), reader.UUID())
	assert.Equal(t, writer.CTime(), reader.CTime())

	assert.Equal(t, uint64(1000), reader.ChunkEnd(0))
	assert.Equal(t, uint64(2500), reader.ChunkEnd(1))
	assert.Equal(t, uint64(4000), reader.ChunkEnd(2))

	info0 := reader.ChunkInfo(0)
	require.NotNil(t, info0)
	assert.Equal(t, uint64(0), info0.Start)
	assert.Equal(t, uint64(1000), info0.End)
	assert.Equal(t, digest1, info0.Digest)

	info1 := reader.ChunkInfo(1)
	require.NotNil(t, info1)
	assert.Equal(t, uint64(1000), info1.Start)
	assert.Equal(t, uint64(2500), info1.End)
	assert.Equal(t, digest2, info1.Digest)

	idx, chunkOffset, err := reader.ChunkFromOffset(1500)
	require.NoError(t, err)
	assert.Equal(t, 1, idx)
	assert.Equal(t, uint64(500), chunkOffset)

	idx, chunkOffset, err = reader.ChunkFromOffset(3500)
	require.NoError(t, err)
	assert.Equal(t, 2, idx)
	assert.Equal(t, uint64(1000), chunkOffset)

	err = reader.VerifyChecksum()
	assert.NoError(t, err)
}

func TestFixedIndexRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.fidx")

	size := uint64(1024 * 1024 * 10)
	chunkSize := uint64(1024 * 1024)

	writer, err := NewFixedIndexWriter(indexPath, size, chunkSize)
	require.NoError(t, err)

	digests := make([][32]byte, 10)
	for i := range digests {
		digests[i] = Sha256Sum([]byte(fmt.Sprintf("chunk%d", i)))
		err = writer.AddChunk(i, digests[i])
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	reader, err := OpenFixedIndex(indexPath)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, 10, reader.ChunkCount())
	assert.Equal(t, size, reader.Size())
	assert.Equal(t, chunkSize, reader.ChunkSize())
	assert.Equal(t, writer.UUID(), reader.UUID())
	assert.Equal(t, writer.CTime(), reader.CTime())

	for i := 0; i < 10; i++ {
		digest := reader.ChunkDigest(i)
		require.NotNil(t, digest)
		assert.Equal(t, digests[i], *digest)

		expectedOffset := uint64(i) * chunkSize
		assert.Equal(t, expectedOffset, reader.ChunkOffset(i))
	}
}

func TestChunkStoreRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	chunksDir := filepath.Join(tmpDir, ".chunks")
	err := os.MkdirAll(chunksDir, 0755)
	require.NoError(t, err)

	store := NewChunkStoreWriter(tmpDir, nil, true)

	data1 := []byte("this is test chunk data 1")
	digest1, err := store.WriteChunk(data1)
	require.NoError(t, err)
	assert.Len(t, digest1, 32)

	data2 := []byte("this is test chunk data 2")
	digest2, err := store.WriteChunk(data2)
	require.NoError(t, err)

	hexDigest1 := fmt.Sprintf("%x", digest1[:])
	chunkPath1 := filepath.Join(chunksDir, hexDigest1[:4], hexDigest1)
	_, err = os.Stat(chunkPath1)
	assert.NoError(t, err, "chunk file should exist")

	digest1Again, err := store.WriteChunk(data1)
	require.NoError(t, err)
	assert.Equal(t, digest1, digest1Again, "same data should produce same digest")

	reader := NewChunkStore(tmpDir, nil, false)
	readData, err := reader.ReadChunk(digest1)
	require.NoError(t, err)
	assert.Equal(t, data1, readData)

	readData2, err := reader.ReadChunk(digest2)
	require.NoError(t, err)
	assert.Equal(t, data2, readData2)
}

func TestBufferedReader(t *testing.T) {
	tmpDir := t.TempDir()

	indexPath := filepath.Join(tmpDir, "test.didx")
	chunksDir := filepath.Join(tmpDir, ".chunks")
	err := os.MkdirAll(chunksDir, 0755)
	require.NoError(t, err)

	store := NewChunkStoreWriter(tmpDir, nil, false)

	indexWriter, err := NewDynamicIndexWriter(indexPath)
	require.NoError(t, err)

	chunkWriter := NewChunkWriter(store, indexWriter, 1024)

	testData := make([]byte, 2500)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	n, err := chunkWriter.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = chunkWriter.Close()
	require.NoError(t, err)

	reader, err := OpenDynamicIndex(indexPath)
	require.NoError(t, err)
	defer reader.Close()

	bufReader := NewBufferedReader(reader, NewChunkStore(tmpDir, nil, false))
	assert.Equal(t, uint64(2500), bufReader.Size())

	readBuf := make([]byte, 100)
	n, err = bufReader.ReadAt(readBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 100, n)
	assert.Equal(t, testData[:100], readBuf)

	n, err = bufReader.ReadAt(readBuf, 1200)
	require.NoError(t, err)
	assert.Equal(t, 100, n)
	assert.Equal(t, testData[1200:1300], readBuf)

	largeBuf := make([]byte, 3000)
	n, err = bufReader.ReadAt(largeBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 2500, n)
	assert.Equal(t, testData, largeBuf[:2500])
}

func TestXAttrParsing(t *testing.T) {
	name := []byte("user.test")
	value := []byte("test value")

	xattr := NewXAttr(name, value)
	assert.Equal(t, name, xattr.Name())
	assert.Equal(t, value, xattr.Value())
}

func TestStatParsing(t *testing.T) {
	stat := Stat{
		Mode:  0755 | ModeIFDIR,
		Flags: 0,
		UID:   1000,
		GID:   1000,
		Mtime: StatxTimestamp{
			Secs:  1700000000,
			Nanos: 123456789,
		},
	}

	assert.Equal(t, ModeIFDIR, stat.FileType())
	assert.Equal(t, uint64(0755), stat.FileMode())
	assert.True(t, stat.IsDir())
	assert.False(t, stat.IsRegularFile())
	assert.False(t, stat.IsSymlink())

	stat.Mode = 0644 | ModeIFREG
	assert.Equal(t, ModeIFREG, stat.FileType())
	assert.False(t, stat.IsDir())
	assert.True(t, stat.IsRegularFile())

	stat.Mode = 0777 | ModeIFLNK
	assert.True(t, stat.IsSymlink())
	assert.False(t, stat.IsRegularFile())

	stat.Mode = ModeIFCHR
	assert.True(t, stat.IsDevice())

	stat.Mode = ModeIFIFO
	assert.True(t, stat.IsFifo())

	stat.Mode = ModeIFSOCK
	assert.True(t, stat.IsSocket())
}

func TestPayloadRef(t *testing.T) {
	ref := PayloadRef{
		Offset: 1024,
		Size:   4096,
	}

	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], ref.Offset)
	binary.LittleEndian.PutUint64(data[8:16], ref.Size)

	readOffset := binary.LittleEndian.Uint64(data[0:8])
	readSize := binary.LittleEndian.Uint64(data[8:16])

	assert.Equal(t, ref.Offset, readOffset)
	assert.Equal(t, ref.Size, readSize)
}

func TestAccessorReadHeader(t *testing.T) {
	headerData := make([]byte, PxarHeaderSize)
	binary.LittleEndian.PutUint64(headerData[0:8], PXAREntry)
	binary.LittleEndian.PutUint64(headerData[8:16], 256)

	reader := &testReaderAt{data: headerData}

	accessor := &Accessor{
		metaReader: reader,
	}

	header, _, err := accessor.readHeader(0)
	require.NoError(t, err)
	assert.Equal(t, PXAREntry, header.HType)
	assert.Equal(t, uint64(256), header.FullSize)
}

func TestHeaderSize(t *testing.T) {
	uncompressed := UncompressedBlobMagic10
	assert.Equal(t, DataBlobHeaderSize, HeaderSize(&uncompressed))

	compressed := CompressedBlobMagic10
	assert.Equal(t, DataBlobHeaderSize, HeaderSize(&compressed))

	encrypted := EncryptedBlobMagic10
	assert.Equal(t, EncryptedDataBlobHeaderSize, HeaderSize(&encrypted))

	encCompressed := EncryptedCompressedBlobMagic10
	assert.Equal(t, EncryptedDataBlobHeaderSize, HeaderSize(&encCompressed))
}

func TestIsEncryptedBlob(t *testing.T) {
	assert.False(t, IsEncryptedBlob(&UncompressedBlobMagic10))
	assert.False(t, IsEncryptedBlob(&CompressedBlobMagic10))
	assert.True(t, IsEncryptedBlob(&EncryptedBlobMagic10))
	assert.True(t, IsEncryptedBlob(&EncryptedCompressedBlobMagic10))
}

func TestIsCompressedBlob(t *testing.T) {
	assert.False(t, IsCompressedBlob(&UncompressedBlobMagic10))
	assert.True(t, IsCompressedBlob(&CompressedBlobMagic10))
	assert.False(t, IsCompressedBlob(&EncryptedBlobMagic10))
	assert.True(t, IsCompressedBlob(&EncryptedCompressedBlobMagic10))
}

func TestCompressData(t *testing.T) {
	testData := bytes.Repeat([]byte("hello world this is test data "), 100)

	compressed, err := CompressData(testData)
	require.NoError(t, err)

	assert.Less(t, len(compressed), len(testData), "compressed data should be smaller for repetitive content")

	blob := &DataBlob{rawData: make([]byte, DataBlobHeaderSize+len(compressed))}
	copy(blob.rawData[0:8], CompressedBlobMagic10[:])
	copy(blob.rawData[DataBlobHeaderSize:], compressed)

	decoded, err := blob.decodeCompressed(nil)
	require.NoError(t, err)
	assert.Equal(t, testData, decoded)
}

func TestChunker(t *testing.T) {
	chunker := NewChunker(4 * 1024 * 1024)

	smallData := make([]byte, 1000)
	boundaries := chunker.ChunkBoundaries(smallData)
	assert.Len(t, boundaries, 1)
	assert.Equal(t, len(smallData), boundaries[0])

	largeData := make([]byte, 10*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	boundaries = chunker.ChunkBoundaries(largeData)
	assert.Greater(t, len(boundaries), 1)

	totalSize := 0
	for _, b := range boundaries {
		totalSize = b
	}
	assert.Equal(t, len(largeData), totalSize)
}

type testReaderAt struct {
	data []byte
}

func (r *testReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[offset:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *testReaderAt) Size() uint64 {
	return uint64(len(r.data))
}

func TestAccessorWithRealPxarData(t *testing.T) {
	pxarData := buildTestPxarArchive(t)

	reader := &testReaderAt{data: pxarData}

	accessor := &Accessor{
		metaReader:    reader,
		payloadReader: reader,
		metaSize:      uint64(len(pxarData)),
		entries:       make(map[uint64]*FileEntry),
		dirCache:      xsync.NewMapOf[uint64, []FileEntry](),
	}

	root, err := accessor.OpenRoot()
	require.NoError(t, err)
	require.NotNil(t, root)
}

func buildTestPxarArchive(t *testing.T) []byte {
	var buf bytes.Buffer

	writeHeader := func(htype uint64, contentSize uint64) {
		header := make([]byte, PxarHeaderSize)
		binary.LittleEndian.PutUint64(header[0:8], htype)
		binary.LittleEndian.PutUint64(header[8:16], PxarHeaderSize+contentSize)
		buf.Write(header)
	}

	writeHeader(PXARFormatVersion, 0)

	writeHeader(PXAREntry, 36)
	stat := make([]byte, 36)
	binary.LittleEndian.PutUint64(stat[0:8], 0755|ModeIFDIR)
	binary.LittleEndian.PutUint64(stat[8:16], 0)
	binary.LittleEndian.PutUint32(stat[16:20], 0)
	binary.LittleEndian.PutUint32(stat[20:24], 0)
	binary.LittleEndian.PutUint64(stat[24:32], 1700000000)
	binary.LittleEndian.PutUint32(stat[32:36], 0)
	buf.Write(stat)

	filename := []byte{0}
	writeHeader(PXARFilename, uint64(len(filename)))
	buf.Write(filename)

	return buf.Bytes()
}
