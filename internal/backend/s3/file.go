//go:build linux

package s3fs

import (
	"context"
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3File struct {
	fs     *S3FS
	key    string
	offset int64
	size   int64
}

func (f *S3File) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, syscall.EINVAL
	}

	ctx, cancel := context.WithTimeout(f.fs.ctx, 30*time.Second)
	defer cancel()

	// Calculate read-ahead size based on buffer size and configured read-ahead
	readAheadBytes := int64(len(buf))
	if readAheadBytes < readAheadSize {
		readAheadBytes = readAheadSize
	}

	// Use range request with read-ahead
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, off+readAheadBytes-1)

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeHeader),
	}

	result, err := f.fs.client.GetObject(ctx, getInput)
	if err != nil {
		return 0, err
	}
	defer result.Body.Close()

	// Read only what was requested into the buffer
	n, err := io.ReadFull(result.Body, buf)

	// Handle partial reads at end of file
	if err == io.ErrUnexpectedEOF {
		return n, io.EOF
	}

	if err != nil {
		return n, err
	}

	// If we read less than requested, it means we hit EOF
	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}

func (f *S3File) Close() error {
	return nil
}

