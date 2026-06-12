package pxar

import (
	"bytes"
	"io"
	"testing"
)

type bytesReaderAt struct {
	data []byte
}

func (r bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type readSeekCloser struct {
	io.ReadSeeker
}

func (readSeekCloser) Close() error { return nil }

func newReadSeekCloser(data []byte) io.ReadCloser {
	section := io.NewSectionReader(bytesReaderAt{data}, 0, int64(len(data)))
	return readSeekCloser{ReadSeeker: section}
}

func TestReadSeekCloserPreservesSeeker(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i)
	}

	rc := newReadSeekCloser(data)

	seeker, ok := rc.(io.Seeker)
	if !ok {
		t.Fatal("readSeekCloser must preserve io.Seeker")
	}

	_, err := seeker.Seek(512<<10, io.SeekStart)
	if err != nil {
		t.Fatalf("seek: %v", err)
	}
	buf := make([]byte, 4<<10)
	n, err := io.ReadFull(rc, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 4<<10 {
		t.Fatalf("got %d bytes, want %d", n, 4<<10)
	}
	if !bytes.Equal(buf, data[512<<10:512<<10+4<<10]) {
		t.Fatal("data mismatch after seek")
	}
}

func TestReadContentAtSizes(t *testing.T) {
	sizes := []int64{
		0,
		1,
		100,
		4<<20 - 1,
		4 << 20,
		4<<20 + 1,
		8 << 20,
		12<<20 + 37,
		20 << 20,
	}

	for _, fileSize := range sizes {
		t.Run("", func(t *testing.T) {
			data := make([]byte, fileSize)
			for i := range data {
				data[i] = byte(i % 251)
			}

			rc := newReadSeekCloser(data)
			seeker, ok := rc.(io.Seeker)
			if !ok {
				t.Fatal("readSeekCloser must preserve io.Seeker")
			}

			chunkSize := 4 << 20
			var totalRead int64
			buf := make([]byte, chunkSize)

			for totalRead < fileSize {
				remaining := fileSize - totalRead
				reqLen := min(int64(chunkSize), remaining)

				if _, err := seeker.Seek(totalRead, io.SeekStart); err != nil {
					t.Fatalf("seek to %d: %v", totalRead, err)
				}

				n, err := io.ReadFull(rc, buf[:reqLen])
				if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
					t.Fatalf("read at %d: %v", totalRead, err)
				}

				if !bytes.Equal(buf[:n], data[totalRead:totalRead+int64(n)]) {
					t.Fatalf("data mismatch at offset %d", totalRead)
				}

				totalRead += int64(n)
			}

			if totalRead != fileSize {
				t.Fatalf("read %d bytes, want %d", totalRead, fileSize)
			}
		})
	}
}
