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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/crc64nvme"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"
)

type closingHash struct {
	hash.Hash
}

func (*closingHash) Close() {}

func newSecureMinioClient(t *testing.T, handler http.Handler, trailingHeaders bool) *minio.Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := minio.New(strings.TrimPrefix(server.URL, "https://"), &minio.Options{
		Creds:           credentials.NewStaticV4(testAccessKey, testSecretKey, ""),
		Region:          "us-west-2",
		Secure:          true,
		Transport:       server.Client().Transport,
		TrailingHeaders: trailingHeaders,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestUnsignedPayloadOverTLS covers the payload modes clients switch to on HTTPS.
func TestUnsignedPayloadOverTLS(t *testing.T) {
	for _, test := range []struct {
		name            string
		trailingHeaders bool
		options         minio.PutObjectOptions
		want            string
	}{
		{name: "unsigned payload", want: unsignedPayloadHash},
		{name: "unsigned trailer", trailingHeaders: true, options: minio.PutObjectOptions{Checksum: minio.ChecksumCRC32C}, want: unsignedTrailerPayloadHash},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, _, _ := newRoundTripHandler(t)
			var uploadPayloadHash string
			capture := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					uploadPayloadHash = r.Header.Get("X-Amz-Content-Sha256")
				}
				handler.ServeHTTP(w, r)
			})
			client := newSecureMinioClient(t, capture, test.trailingHeaders)

			body := []byte("unsigned payload database backup")
			info, err := client.PutObject(t.Context(), "mariadb", "backup.sql.gz", bytes.NewReader(body), int64(len(body)), test.options)
			if err != nil {
				t.Fatalf("PutObject() error = %v", err)
			}
			if uploadPayloadHash != test.want {
				t.Fatalf("upload payload hash = %q, want %q", uploadPayloadHash, test.want)
			}
			sum := sha256.Sum256(body)
			if info.ETag != hex.EncodeToString(sum[:]) {
				t.Fatalf("etag = %q, want %q", info.ETag, hex.EncodeToString(sum[:]))
			}

			object, err := client.GetObject(t.Context(), "mariadb", "backup.sql.gz", minio.GetObjectOptions{})
			if err != nil {
				t.Fatalf("GetObject() error = %v", err)
			}
			defer object.Close()
			got, err := io.ReadAll(object)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("object = %q, err = %v", got, err)
			}
		})
	}
}

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

func TestUnsignedTrailerRejectsBadChecksum(t *testing.T) {
	payload := []byte("database backup")
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))^1)

	var body bytes.Buffer
	body.WriteString(strconv.FormatInt(int64(len(payload)), 16) + "\r\n")
	body.Write(payload)
	body.WriteString("\r\n0\r\nx-amz-checksum-crc32c:" + base64.StdEncoding.EncodeToString(sum[:]) + "\n\r\n\r\n")
	request := httptest.NewRequest(http.MethodPut, "http://s3.test/mariadb/backup.sql.gz", bytes.NewReader(body.Bytes()))
	request.Header.Set("X-Amz-Content-Sha256", unsignedTrailerPayloadHash)
	request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")

	reader, _, err := newVerifiedPayload(request, testConfig().Credentials[0])
	if err != nil {
		t.Fatalf("new verified payload: %v", err)
	}
	if _, err := io.ReadAll(reader); err == nil || !strings.Contains(err.Error(), "crc32c checksum does not match") {
		t.Fatalf("read error = %v", err)
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

// signedStreamingRequestNoEncoding mirrors how current minio-go/mc stream:
// the aws-chunked framing is identified by the payload hash and
// x-amz-decoded-content-length alone, with no Content-Encoding header signed
// or sent.
func signedStreamingRequestNoEncoding(t *testing.T, payload []byte) *http.Request {
	t.Helper()
	requestTime := time.Now().UTC()
	region := "us-west-2"
	scope := requestTime.Format("20060102") + "/" + region + "/s3/aws4_request"
	request := httptest.NewRequest(http.MethodPut, "http://s3.test/mariadb/backup.sql.gz", nil)
	request.Header.Set("X-Amz-Content-Sha256", streamingPayloadHash)
	request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
	request.Header.Set("X-Amz-Date", requestTime.Format(signatureTimeFormat))
	request = signer.SignV4(*request, testAccessKey, testSecretKey, "", region)
	authorization := request.Header.Get("Authorization")
	requestSignature := authorization[strings.LastIndex(authorization, "Signature=")+len("Signature="):]

	key := sumHMAC([]byte("AWS4"+testSecretKey), []byte(requestTime.Format("20060102")))
	key = sumHMAC(key, []byte(region))
	key = sumHMAC(key, []byte("s3"))
	key = sumHMAC(key, []byte("aws4_request"))
	emptySum := sha256.Sum256(nil)
	emptyHash := hex.EncodeToString(emptySum[:])
	previous := requestSignature
	var framed bytes.Buffer
	chunks := [][]byte{payload}
	for _, chunk := range chunks {
		chunkSum := sha256.Sum256(chunk)
		stringToSign := strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD",
			requestTime.Format(signatureTimeFormat),
			scope,
			previous,
			emptyHash,
			hex.EncodeToString(chunkSum[:]),
		}, "\n")
		signature := hex.EncodeToString(sumHMAC(key, []byte(stringToSign)))
		previous = signature
		framed.WriteString(strconv.FormatInt(int64(len(chunk)), 16))
		framed.WriteString(";chunk-signature=")
		framed.WriteString(signature)
		framed.WriteString("\r\n")
		framed.Write(chunk)
		framed.WriteString("\r\n")
	}
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		requestTime.Format(signatureTimeFormat),
		scope,
		previous,
		emptyHash,
		emptyHash,
	}, "\n")
	finalSignature := hex.EncodeToString(sumHMAC(key, []byte(stringToSign)))
	framed.WriteString("0;chunk-signature=")
	framed.WriteString(finalSignature)
	framed.WriteString("\r\n\r\n")
	request.Body = io.NopCloser(bytes.NewReader(framed.Bytes()))
	request.ContentLength = int64(framed.Len())
	return request
}
