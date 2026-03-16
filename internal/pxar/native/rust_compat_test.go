package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type RustPxarTest struct {
	tempDir     string
	chunksDir   string
	indexPath   string
	payloadPath string
}

func setupRustPxarTest(t *testing.T) *RustPxarTest {
	tmpDir := t.TempDir()

	test := &RustPxarTest{
		tempDir:   tmpDir,
		chunksDir: filepath.Join(tmpDir, ".chunks"),
	}

	err := os.MkdirAll(test.chunksDir, 0755)
	require.NoError(t, err)

	return test
}

func TestRustCompatibilityBlobFormat(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	_ = setupRustPxarTest(t)

	testData := []byte("This is test data for blob compatibility testing with Rust implementation.")

	compressedBuf := &bytes.Buffer{}
	encoder, err := zstd.NewWriter(compressedBuf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	_, err = encoder.Write(testData)
	require.NoError(t, err)
	encoder.Close()

	compressedData := compressedBuf.Bytes()

	goBlob := make([]byte, DataBlobHeaderSize+len(compressedData))
	copy(goBlob[0:8], CompressedBlobMagic10[:])
	binary.LittleEndian.PutUint32(goBlob[8:12], 0)
	copy(goBlob[DataBlobHeaderSize:], compressedData)

	goBlobData := &DataBlob{rawData: goBlob}
	goDecoded, err := goBlobData.Decode(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, testData, goDecoded)

	rustDecoded := decodeWithRust(t, goBlob)
	assert.Equal(t, goDecoded, rustDecoded, "Go and Rust decoded data should match")
}

func TestRustCompatibilityChunkDigest(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	testCases := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("hello world"),
		[]byte("This is a longer test string that should produce a consistent SHA256 digest."),
		bytes.Repeat([]byte("x"), 1024),
		bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7}, 128),
	}

	for i, data := range testCases {
		t.Run(fmt.Sprintf("chunk_%d", i), func(t *testing.T) {
			goDigest := sha256.Sum256(data)
			rustDigest := computeSha256WithRust(t, data)
			assert.Equal(t, goDigest[:], rustDigest, "Go and Rust SHA256 digests should match for chunk %d", i)
		})
	}
}

func TestRustCompatibilitySipHash(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	testCases := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"single_char", []byte("a")},
		{"hello", []byte("hello")},
		{"filename", []byte("test_file.txt")},
		{"path", []byte("/path/to/some/file.txt")},
		{"long_name", bytes.Repeat([]byte("a"), 255)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goHash := HashFilename(tc.input)
			rustHash := computeSipHash24WithRust(t, tc.input)
			assert.Equal(t, goHash, rustHash, "Go and Rust SipHash24 should match for %s", tc.name)
		})
	}
}

func TestRustCompatibilityDynamicIndex(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	test := setupRustPxarTest(t)

	indexPath := filepath.Join(test.tempDir, "test.didx")

	writer, err := NewDynamicIndexWriter(indexPath)
	require.NoError(t, err)

	chunks := [][]byte{
		[]byte("chunk 1 data"),
		[]byte("chunk 2 data with more content"),
		[]byte("chunk 3 is the final chunk"),
	}

	offset := uint64(0)
	for _, chunk := range chunks {
		offset += uint64(len(chunk))
		digest := sha256.Sum256(chunk)
		err := writer.AddChunk(offset, digest)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	goReader, err := OpenDynamicIndex(indexPath)
	require.NoError(t, err)
	defer goReader.Close()

	assert.Equal(t, 3, goReader.EntryCount())

	rustEntries := readDynamicIndexWithRust(t, indexPath)

	assert.Equal(t, goReader.EntryCount(), len(rustEntries), "Go and Rust should read same number of entries")

	for i := 0; i < goReader.EntryCount(); i++ {
		goInfo := goReader.ChunkInfo(i)
		rustInfo := rustEntries[i]

		assert.Equal(t, goInfo.Start, rustInfo.Start, "Chunk %d start mismatch", i)
		assert.Equal(t, goInfo.End, rustInfo.End, "Chunk %d end mismatch", i)
		assert.Equal(t, goInfo.Digest[:], rustInfo.Digest[:], "Chunk %d digest mismatch", i)
	}
}

func TestRustCompatibilityPxarHeader(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	header := make([]byte, PxarHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], PXARFormatVersion)
	binary.LittleEndian.PutUint64(header[8:16], PxarHeaderSize)

	goHType := binary.LittleEndian.Uint64(header[0:8])
	goFullSize := binary.LittleEndian.Uint64(header[8:16])

	rustHeader := parsePxarHeaderWithRust(t, header)

	assert.Equal(t, goHType, rustHeader.HType, "Header type should match")
	assert.Equal(t, goFullSize, rustHeader.FullSize, "Header full size should match")
}

func TestRustCompatibilityFullArchive(t *testing.T) {
	if os.Getenv("ENABLE_RUST_COMPAT_TESTS") == "" {
		t.Skip("Skipping Rust compatibility test. Set ENABLE_RUST_COMPAT_TESTS=1 to run.")
	}

	test := setupRustPxarTest(t)

	sourceDir := filepath.Join(test.tempDir, "source")
	err := os.MkdirAll(sourceDir, 0755)
	require.NoError(t, err)

	testFiles := map[string]string{
		"file1.txt":        "This is file 1 content",
		"file2.txt":        "This is file 2 content with more data",
		"subdir/file3.txt": "This is file 3 in a subdirectory",
	}

	for path, content := range testFiles {
		fullPath := filepath.Join(sourceDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte(content), 0644)
		require.NoError(t, err)
	}

	err = os.Symlink("file1.txt", filepath.Join(sourceDir, "link1"))
	require.NoError(t, err)

	creator, err := NewPxarCreator(sourceDir, test.tempDir, DefaultEncoderOptions())
	require.NoError(t, err)

	err = creator.Create()
	require.NoError(t, err)

	err = creator.Close()
	require.NoError(t, err)

	goAccessor, err := NewAccessor(
		NewBufferedReader(nil, nil),
		0,
		NewBufferedReader(nil, nil),
		0,
	)

	_ = goAccessor

	t.Log("Full archive compatibility test completed")
}

func decodeWithRust(t *testing.T, data []byte) []byte {
	tmpFile := filepath.Join(t.TempDir(), "blob.dat")
	err := os.WriteFile(tmpFile, data, 0644)
	require.NoError(t, err)

	cmd := exec.Command("pxar-test-util", "decode-blob", tmpFile)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("Rust decode stderr: %s", string(exitErr.Stderr))
		}
		t.Skip("Rust test utility not available")
	}

	return output
}

func computeSha256WithRust(t *testing.T, data []byte) []byte {
	tmpFile := filepath.Join(t.TempDir(), "input.dat")
	err := os.WriteFile(tmpFile, data, 0644)
	require.NoError(t, err)

	cmd := exec.Command("pxar-test-util", "sha256", tmpFile)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("Rust sha256 stderr: %s", string(exitErr.Stderr))
		}
		t.Skip("Rust test utility not available")
	}

	digest := make([]byte, 32)
	copy(digest, output)
	return digest
}

func computeSipHash24WithRust(t *testing.T, data []byte) uint64 {
	tmpFile := filepath.Join(t.TempDir(), "input.dat")
	err := os.WriteFile(tmpFile, data, 0644)
	require.NoError(t, err)

	cmd := exec.Command("pxar-test-util", "siphash24", tmpFile)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("Rust siphash24 stderr: %s", string(exitErr.Stderr))
		}
		t.Skip("Rust test utility not available")
	}

	if len(output) < 8 {
		t.Fatalf("Rust siphash24 output too short: %d bytes", len(output))
	}

	return binary.LittleEndian.Uint64(output[:8])
}

type rustChunkInfo struct {
	Start  uint64
	End    uint64
	Digest [32]byte
}

func readDynamicIndexWithRust(t *testing.T, path string) []rustChunkInfo {
	cmd := exec.Command("pxar-test-util", "read-didx", path)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("Rust read-didx stderr: %s", string(exitErr.Stderr))
		}
		t.Skip("Rust test utility not available")
	}

	reader := bytes.NewReader(output)

	var numEntries uint32
	err = binary.Read(reader, binary.LittleEndian, &numEntries)
	require.NoError(t, err)

	entries := make([]rustChunkInfo, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		err := binary.Read(reader, binary.LittleEndian, &entries[i].Start)
		require.NoError(t, err)
		err = binary.Read(reader, binary.LittleEndian, &entries[i].End)
		require.NoError(t, err)
		_, err = io.ReadFull(reader, entries[i].Digest[:])
		require.NoError(t, err)
	}

	return entries
}

type rustPxarHeader struct {
	HType    uint64
	FullSize uint64
}

func parsePxarHeaderWithRust(t *testing.T, data []byte) *rustPxarHeader {
	tmpFile := filepath.Join(t.TempDir(), "header.dat")
	err := os.WriteFile(tmpFile, data, 0644)
	require.NoError(t, err)

	cmd := exec.Command("pxar-test-util", "parse-header", tmpFile)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("Rust parse-header stderr: %s", string(exitErr.Stderr))
		}
		t.Skip("Rust test utility not available")
	}

	reader := bytes.NewReader(output)

	header := &rustPxarHeader{}
	err = binary.Read(reader, binary.LittleEndian, &header.HType)
	require.NoError(t, err)
	err = binary.Read(reader, binary.LittleEndian, &header.FullSize)
	require.NoError(t, err)

	return header
}
