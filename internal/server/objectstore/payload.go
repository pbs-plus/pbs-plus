//go:build linux

package objectstore

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/crc64nvme"
)

const (
	streamingPayloadHash        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingTrailerPayloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	unsignedPayloadHash         = "UNSIGNED-PAYLOAD"
	unsignedTrailerPayloadHash  = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	emptySHA256                 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	maximumChunkHeaderSize      = 1024
)

type verifiedReader struct {
	body     io.ReadCloser
	hash     hash.Hash
	expected [sha256.Size]byte
	length   int64
	read     int64
	done     bool
}

func newVerifiedPayload(r *http.Request, credential Credential) (io.ReadCloser, int64, error) {
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	switch payloadHash {
	case streamingPayloadHash, streamingTrailerPayloadHash:
		return newAWSChunkedReader(r, credential, payloadHash == streamingTrailerPayloadHash, true)
	case unsignedTrailerPayloadHash:
		return newAWSChunkedReader(r, credential, true, false)
	case "", unsignedPayloadHash:
		if r.ContentLength < 0 {
			return nil, 0, fmt.Errorf("content length is required")
		}
		return r.Body, r.ContentLength, nil
	}
	if r.ContentLength < 0 {
		return nil, 0, fmt.Errorf("content length is required")
	}
	var expected [sha256.Size]byte
	if n, err := hex.Decode(expected[:], []byte(payloadHash)); err != nil || n != sha256.Size {
		return nil, 0, fmt.Errorf("invalid payload hash")
	}
	return &verifiedReader{
		body:     r.Body,
		hash:     sha256.New(),
		expected: expected,
		length:   r.ContentLength,
	}, r.ContentLength, nil
}

// isPayloadDigest reports whether X-Amz-Content-Sha256 carries the body's own
// SHA-256 rather than a streaming or unsigned payload marker.
func isPayloadDigest(payloadHash string) bool {
	if len(payloadHash) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(payloadHash)
	return err == nil
}

func (r *verifiedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	n, err := r.body.Read(p)
	if n > 0 {
		r.read += int64(n)
		_, _ = r.hash.Write(p[:n])
		if r.read > r.length {
			return n, fmt.Errorf("payload exceeds content length")
		}
	}
	if err != io.EOF {
		return n, err
	}
	r.done = true
	if r.read != r.length {
		return n, fmt.Errorf("payload length is %d, expected %d", r.read, r.length)
	}
	if subtle.ConstantTimeCompare(r.hash.Sum(nil), r.expected[:]) != 1 {
		return n, fmt.Errorf("payload hash does not match")
	}
	return n, io.EOF
}

func (r *verifiedReader) Close() error {
	return r.body.Close()
}

type awsChunkedReader struct {
	body              io.ReadCloser
	buffer            *bufio.Reader
	requestTime       time.Time
	region            string
	signingKey        []byte
	previousSignature [sha256.Size]byte
	chunkHash         hash.Hash
	chunkSignature    [sha256.Size]byte
	chunkRemaining    int64
	decodedLength     int64
	decoded           int64
	withTrailer       bool
	signed            bool
	trailerNames      map[string]struct{}
	crc32c            hash.Hash32
	crc64nvme         hash.Hash64
	pendingErr        error
	done              bool
}

func newAWSChunkedReader(r *http.Request, credential Credential, withTrailer, signed bool) (io.ReadCloser, int64, error) {
	decodedLength, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if err != nil || decodedLength < 0 {
		return nil, 0, fmt.Errorf("invalid decoded content length")
	}
	reader := &awsChunkedReader{
		body:          r.Body,
		buffer:        bufio.NewReaderSize(r.Body, maximumChunkHeaderSize),
		chunkHash:     sha256.New(),
		decodedLength: decodedLength,
		withTrailer:   withTrailer,
		signed:        signed,
	}
	if signed {
		header, err := parseSignatureHeader(r.Header.Get("Authorization"))
		if err != nil {
			return nil, 0, err
		}
		requestTime, err := time.Parse(signatureTimeFormat, r.Header.Get("X-Amz-Date"))
		if err != nil {
			return nil, 0, fmt.Errorf("invalid x-amz-date")
		}
		reader.requestTime = requestTime
		reader.region = header.region
		reader.signingKey = signingKey(credential.SecretKey, header.date, header.region)
		copy(reader.previousSignature[:], header.signature)
	}
	if err := reader.configureTrailers(r.Header.Values("X-Amz-Trailer")); err != nil {
		return nil, 0, err
	}
	return reader, decodedLength, nil
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil
		return 0, err
	}
	if r.done {
		return 0, io.EOF
	}
	if r.chunkRemaining == 0 {
		if err := r.startChunk(); err != nil {
			return 0, err
		}
		if r.done {
			return 0, io.EOF
		}
	}
	if int64(len(p)) > r.chunkRemaining {
		p = p[:r.chunkRemaining]
	}
	n, err := r.buffer.Read(p)
	if n > 0 {
		r.chunkRemaining -= int64(n)
		r.decoded += int64(n)
		_, _ = r.chunkHash.Write(p[:n])
		r.writeChecksums(p[:n])
	}
	if err != nil {
		return n, fmt.Errorf("read aws chunk: %w", err)
	}
	if r.decoded > r.decodedLength {
		return n, fmt.Errorf("payload exceeds decoded content length")
	}
	if r.chunkRemaining == 0 {
		if err := r.finishChunk(); err != nil {
			if n > 0 {
				r.pendingErr = err
				return n, nil
			}
			return 0, err
		}
	}
	return n, nil
}

func (r *awsChunkedReader) Close() error {
	return r.body.Close()
}

func (r *awsChunkedReader) startChunk() error {
	line, err := r.readLine()
	if err != nil {
		return fmt.Errorf("read aws chunk header: %w", err)
	}
	sizeText, signatureText, ok := strings.Cut(string(line), ";chunk-signature=")
	if ok != r.signed || strings.Contains(signatureText, ";") {
		return fmt.Errorf("invalid aws chunk header")
	}
	size, err := strconv.ParseInt(sizeText, 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("invalid aws chunk size")
	}
	if r.signed {
		if n, err := hex.Decode(r.chunkSignature[:], []byte(signatureText)); err != nil || n != sha256.Size {
			return fmt.Errorf("invalid aws chunk signature")
		}
	}
	r.chunkRemaining = size
	r.chunkHash.Reset()
	if size != 0 {
		return nil
	}
	if err := r.verifyChunk(); err != nil {
		return err
	}
	if r.decoded != r.decodedLength {
		return fmt.Errorf("payload length is %d, expected %d", r.decoded, r.decodedLength)
	}
	if r.withTrailer {
		if err := r.readTrailers(); err != nil {
			return err
		}
	} else if err := r.expectCRLF(); err != nil {
		return err
	}
	r.done = true
	return nil
}

func (r *awsChunkedReader) finishChunk() error {
	if err := r.expectCRLF(); err != nil {
		return err
	}
	return r.verifyChunk()
}

func (r *awsChunkedReader) verifyChunk() error {
	if !r.signed {
		return nil
	}
	checksum := hex.EncodeToString(r.chunkHash.Sum(nil))
	scope := strings.Join([]string{r.requestTime.Format("20060102"), r.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		r.requestTime.Format(signatureTimeFormat),
		scope,
		hex.EncodeToString(r.previousSignature[:]),
		emptySHA256,
		checksum,
	}, "\n")
	expected := sumHMAC(r.signingKey, []byte(stringToSign))
	if subtle.ConstantTimeCompare(r.chunkSignature[:], expected) != 1 {
		return fmt.Errorf("aws chunk signature does not match")
	}
	copy(r.previousSignature[:], r.chunkSignature[:])
	return nil
}

func (r *awsChunkedReader) configureTrailers(values []string) error {
	if !r.withTrailer {
		if len(values) != 0 {
			return fmt.Errorf("trailers require streaming trailer payload signing")
		}
		return nil
	}
	r.trailerNames = make(map[string]struct{})
	for _, value := range values {
		for name := range strings.SplitSeq(value, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				return fmt.Errorf("invalid trailer name")
			}
			if _, exists := r.trailerNames[name]; exists {
				return fmt.Errorf("duplicate trailer %q", name)
			}
			r.trailerNames[name] = struct{}{}
			switch name {
			case "x-amz-checksum-crc32c":
				r.crc32c = crc32.New(crc32.MakeTable(crc32.Castagnoli))
			case "x-amz-checksum-crc64nvme":
				r.crc64nvme = crc64nvme.New()
			default:
				return fmt.Errorf("unsupported trailer %q", name)
			}
		}
	}
	if len(r.trailerNames) == 0 {
		return fmt.Errorf("streaming trailer is missing")
	}
	return nil
}

func (r *awsChunkedReader) readTrailers() error {
	canonical := sha256.New()
	seen := make(map[string]struct{}, len(r.trailerNames))
	values := make(map[string]string, len(r.trailerNames))
	for range len(r.trailerNames) {
		line, err := r.readLine()
		if err != nil {
			return fmt.Errorf("read aws trailer: %w", err)
		}
		name, value, ok := strings.Cut(string(line), ":")
		name = strings.ToLower(name)
		if !ok {
			return fmt.Errorf("invalid aws trailer")
		}
		if _, declared := r.trailerNames[name]; !declared {
			return fmt.Errorf("undeclared trailer %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate trailer %q", name)
		}
		seen[name] = struct{}{}
		values[name] = value
		_, _ = io.WriteString(canonical, name+":"+value+"\n")
	}
	if err := r.expectCRLF(); err != nil {
		return err
	}
	if !r.signed {
		if err := r.verifyChecksums(values); err != nil {
			return err
		}
		return r.expectCRLF()
	}
	line, err := r.readLine()
	if err != nil {
		return fmt.Errorf("read aws trailer signature: %w", err)
	}
	name, signatureText, ok := strings.Cut(string(line), ":")
	if !ok || strings.ToLower(name) != "x-amz-trailer-signature" {
		return fmt.Errorf("invalid aws trailer signature")
	}
	var trailerSignature [sha256.Size]byte
	if n, err := hex.Decode(trailerSignature[:], []byte(signatureText)); err != nil || n != sha256.Size {
		return fmt.Errorf("invalid aws trailer signature")
	}
	scope := strings.Join([]string{r.requestTime.Format("20060102"), r.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-TRAILER",
		r.requestTime.Format(signatureTimeFormat),
		scope,
		hex.EncodeToString(r.previousSignature[:]),
		hex.EncodeToString(canonical.Sum(nil)),
	}, "\n")
	expected := sumHMAC(r.signingKey, []byte(stringToSign))
	if subtle.ConstantTimeCompare(trailerSignature[:], expected) != 1 {
		return fmt.Errorf("aws trailer signature does not match")
	}
	if err := r.verifyChecksums(values); err != nil {
		return err
	}
	return r.expectCRLF()
}

func (r *awsChunkedReader) verifyChecksums(values map[string]string) error {
	if r.crc32c != nil {
		var checksum [4]byte
		binary.BigEndian.PutUint32(checksum[:], r.crc32c.Sum32())
		if base64.StdEncoding.EncodeToString(checksum[:]) != values["x-amz-checksum-crc32c"] {
			return fmt.Errorf("crc32c checksum does not match")
		}
	}
	if r.crc64nvme != nil {
		var checksum [8]byte
		binary.BigEndian.PutUint64(checksum[:], r.crc64nvme.Sum64())
		if base64.StdEncoding.EncodeToString(checksum[:]) != values["x-amz-checksum-crc64nvme"] {
			return fmt.Errorf("crc64nvme checksum does not match")
		}
	}
	return nil
}

func (r *awsChunkedReader) writeChecksums(p []byte) {
	if r.crc32c != nil {
		_, _ = r.crc32c.Write(p)
	}
	if r.crc64nvme != nil {
		_, _ = r.crc64nvme.Write(p)
	}
}

func (r *awsChunkedReader) readLine() ([]byte, error) {
	line, err := r.buffer.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) > maximumChunkHeaderSize {
		return nil, fmt.Errorf("line is too long")
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func (r *awsChunkedReader) expectCRLF() error {
	first, err := r.buffer.ReadByte()
	if err != nil {
		return fmt.Errorf("read aws chunk terminator: %w", err)
	}
	second, err := r.buffer.ReadByte()
	if err != nil || first != '\r' || second != '\n' {
		return fmt.Errorf("invalid aws chunk terminator")
	}
	return nil
}

func headerContainsToken(value, token string) bool {
	for part := range strings.SplitSeq(value, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func signingKey(secret, date, region string) []byte {
	dateKey := sumHMAC([]byte("AWS4"+secret), []byte(date))
	regionKey := sumHMAC(dateKey, []byte(region))
	serviceKey := sumHMAC(regionKey, []byte("s3"))
	return sumHMAC(serviceKey, []byte("aws4_request"))
}
