//go:build linux

package pxar

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	pxarpkg "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

type staticTaskWriter struct{}

func (staticTaskWriter) WriteString(string) {}

func buildLargeFileArchive(t *testing.T, payload []byte) *transfer.FileReader {
	t.Helper()
	dirMeta := &pxarpkg.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | 0o755,
			Mtime: format.NewStatxTimestampFromDuration(1430487000 * time.Second),
		},
	}
	fileMeta := &pxarpkg.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | 0o644,
			Mtime: format.NewStatxTimestampFromDuration(1430487000 * time.Second),
		},
	}

	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta, nil)
	if _, err := enc.AddFile(fileMeta, "big.bin", payload); err != nil {
		t.Fatalf("add file: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close encoder: %v", err)
	}
	return transfer.NewFileReader(bytes.NewReader(buf.Bytes()))
}

func findBigEntry(t *testing.T, fs *vfs.LocalFS) pxarpkg.FileInfo {
	t.Helper()
	root, err := fs.Root()
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if e.Name() == "big.bin" {
			return e
		}
	}
	t.Fatal("big.bin not found in archive")
	return pxarpkg.FileInfo{}
}

type captureStream struct {
	buf bytes.Buffer
}

func (c *captureStream) Write(p []byte) (int, error) { return c.buf.Write(p) }
func (c *captureStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *captureStream) Close() error                { return nil }
func (c *captureStream) SetDeadline(time.Time) error { return nil }

func (c *captureStream) payload() []byte {
	remaining := c.buf.Bytes()
	if len(remaining) < 14 {
		return nil
	}
	length := binary.LittleEndian.Uint64(remaining[6:14])
	if int(length) > len(remaining)-14 {
		return nil
	}
	body := append([]byte(nil), remaining[14:14+int(length)]...)
	c.buf.Next(14 + int(length))
	return body
}

func TestRemoteReadContentHandleStored(t *testing.T) {
	const chunk = 4 << 20
	fileSize := 3 * chunk
	payload := make([]byte, fileSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	ar := buildLargeFileArchive(t, payload)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	entry := findBigEntry(t, fs)
	if entry.ContentRange == nil {
		t.Fatal("big.bin has no content range")
	}

	server, _ := NewRemoteServer(&PxarReader{
		ofs:       fs,
		task:      staticTaskWriter{},
		startTime: time.Now(),
	})
	defer server.Close()

	ctx := context.Background()

	openReq := readContentReq{
		ContentStart: entry.ContentRange[0],
		ContentEnd:   entry.ContentRange[1],
		FileSize:     uint64(fileSize),
		Length:       chunk,
	}
	openReqPayload, err := cbor.Marshal(&openReq)
	if err != nil {
		t.Fatalf("marshal open req: %v", err)
	}

	openResp, rerr := server.handleReadContent(&arpc.Request{Context: ctx, Payload: openReqPayload})
	if rerr != nil {
		t.Fatalf("handleReadContent: %v", rerr)
	}
	if openResp.Status != 213 {
		t.Fatalf("handleReadContent status = %d, want 213", openResp.Status)
	}

	var handle handleIDResp
	if err := cbor.Unmarshal(openResp.Data, &handle); err != nil {
		t.Fatalf("unmarshal handle: %v", err)
	}
	if handle.HandleID == 0 {
		t.Fatal("handle ID must be non-zero")
	}

	got := bytes.Buffer{}
	if openResp.RawStream != nil {
		cs := &captureStream{}
		openResp.RawStream(cs)
		got.Write(cs.payload())
	}

	offset := int64(got.Len())
	for offset < int64(fileSize) {
		reqLen := int64(chunk)
		if offset+reqLen > int64(fileSize) {
			reqLen = int64(fileSize) - offset
		}
		atReq := readContentAtReq{
			HandleID: handle.HandleID,
			Offset:   offset,
			Length:   int(reqLen),
		}
		atReqPayload, merr := cbor.Marshal(&atReq)
		if merr != nil {
			t.Fatalf("marshal at req: %v", merr)
		}
		atResp, aerr := server.handleReadContentAt(&arpc.Request{Context: ctx, Payload: atReqPayload})
		if aerr != nil {
			t.Fatalf("handleReadContentAt offset %d: %v", offset, aerr)
		}
		if atResp.RawStream != nil {
			cs := &captureStream{}
			atResp.RawStream(cs)
			got.Write(cs.payload())
		}
		offset = int64(got.Len())
	}

	closeReq := closeContentReq{HandleID: handle.HandleID}
	closeReqPayload, _ := cbor.Marshal(&closeReq)
	if _, cerr := server.handleCloseContent(&arpc.Request{Context: ctx, Payload: closeReqPayload}); cerr != nil {
		t.Fatalf("handleCloseContent: %v", cerr)
	}

	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("restored content mismatch: got %d bytes, want %d", got.Len(), len(payload))
	}
}
