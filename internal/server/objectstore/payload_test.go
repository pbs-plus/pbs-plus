//go:build linux

package objectstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/crc64nvme"
	"github.com/minio/minio-go/v7/pkg/signer"
)

type closingHash struct {
	hash.Hash
}

func (*closingHash) Close() {}

func TestVerifiedPayload(t *testing.T) {
	payload := []byte("database backup")
	sum := sha256.Sum256(payload)
	request := httptest.NewRequest(http.MethodPut, "http://s3.test/mariadb/backup.sql.gz", bytes.NewReader(payload))
	request.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	request = signer.SignV4(*request, testAccessKey, testSecretKey, "", "us-west-2")
	credential, err := testConfig().authenticate(request, time.Now())
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	reader, length, err := newVerifiedPayload(request, credential)
	if err != nil {
		t.Fatalf("new verified payload: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read verified payload: %v", err)
	}
	if length != int64(len(payload)) || !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, length = %d", got, length)
	}
}

func TestVerifiedPayloadRejectsMismatch(t *testing.T) {
	payload := []byte("database backup")
	sum := sha256.Sum256([]byte("different"))
	request := httptest.NewRequest(http.MethodPut, "http://s3.test/mariadb/backup.sql.gz", bytes.NewReader(payload))
	request.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	reader, _, err := newVerifiedPayload(request, testConfig().Credentials[0])
	if err != nil {
		t.Fatalf("new verified payload: %v", err)
	}
	if _, err := io.ReadAll(reader); err == nil || !strings.Contains(err.Error(), "payload hash does not match") {
		t.Fatalf("read error = %v", err)
	}
}

func TestAWSChunkedPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("chunk-data-"), 10_000)
	request, requestTime := signedStreamingRequest(t, payload, "", "")
	credential, err := testConfig().authenticate(request, requestTime)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	reader, length, err := newVerifiedPayload(request, credential)
	if err != nil {
		t.Fatalf("new verified payload: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read aws-chunked payload: %v", err)
	}
	if length != int64(len(payload)) || !bytes.Equal(got, payload) {
		t.Fatalf("decoded payload length = %d, want %d", length, len(payload))
	}
}

func TestAWSChunkedPayloadRejectsTampering(t *testing.T) {
	payload := []byte("database backup")
	request, requestTime := signedStreamingRequest(t, payload, "", "")
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("read encoded payload: %v", err)
	}
	encoded[bytes.Index(encoded, payload)] ^= 1
	request.Body = io.NopCloser(bytes.NewReader(encoded))
	credential, err := testConfig().authenticate(request, requestTime)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	reader, _, err := newVerifiedPayload(request, credential)
	if err != nil {
		t.Fatalf("new verified payload: %v", err)
	}
	if _, err := io.ReadAll(reader); err == nil || !strings.Contains(err.Error(), "chunk signature does not match") {
		t.Fatalf("read error = %v", err)
	}
}

func TestAWSChunkedChecksumTrailers(t *testing.T) {
	payload := []byte("database backup")
	tests := []struct {
		name   string
		header string
		value  func([]byte) string
	}{
		{
			name:   "crc32c",
			header: "X-Amz-Checksum-Crc32c",
			value: func(data []byte) string {
				var sum [4]byte
				binary.BigEndian.PutUint32(sum[:], crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
				return base64.StdEncoding.EncodeToString(sum[:])
			},
		},
		{
			name:   "crc64nvme",
			header: "X-Amz-Checksum-Crc64nvme",
			value: func(data []byte) string {
				var sum [8]byte
				binary.BigEndian.PutUint64(sum[:], crc64nvme.Checksum(data))
				return base64.StdEncoding.EncodeToString(sum[:])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, requestTime := signedStreamingRequest(t, payload, test.header, test.value(payload))
			credential, err := testConfig().authenticate(request, requestTime)
			if err != nil {
				t.Fatalf("authenticate: %v", err)
			}
			reader, _, err := newVerifiedPayload(request, credential)
			if err != nil {
				t.Fatalf("new verified payload: %v", err)
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read aws-chunked payload: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload = %q", got)
			}
		})
	}
}

func signedStreamingRequest(t *testing.T, payload []byte, trailerName, trailerValue string) (*http.Request, time.Time) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPut, "http://s3.test/mariadb/backup.sql.gz", bytes.NewReader(payload))
	if trailerName != "" {
		request.Trailer = http.Header{trailerName: []string{trailerValue}}
	}
	requestTime := time.Now().UTC()
	return signer.StreamingSignV4(
		request,
		testAccessKey,
		testSecretKey,
		"",
		"us-west-2",
		int64(len(payload)),
		requestTime,
		&closingHash{Hash: sha256.New()},
	), requestTime
}
