package sftp

import "io"

type eofInjectingReaderAt struct {
	file   io.ReaderAt
	length int64
}

func NewEOFInjectingReaderAt(file io.ReaderAt, length int64) io.ReaderAt {
	return &eofInjectingReaderAt{file: file, length: length}
}

func (r *eofInjectingReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.length {
		// If reading beyond the file length, return EOF to simulate a proper end.
		return 0, io.EOF
	}

	// Attempt to read as usual.
	n, err = r.file.ReadAt(p, off)
	if err == io.ErrUnexpectedEOF || n < len(p) {
		// If an unexpected EOF or partial read, return what we have and inject EOF.
		return n, io.EOF
	}

	return n, err
}
