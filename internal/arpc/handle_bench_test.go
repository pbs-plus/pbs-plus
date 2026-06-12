package arpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

const benchChunkSize = 4 << 20

type benchOpenReq struct {
	FileSize uint64 `cbor:"file_size"`
}

type benchOpenResp struct {
	HandleID uint64 `cbor:"handle_id"`
}

type benchReadContentReq struct {
	FileSize uint64 `cbor:"file_size"`
	Length   int    `cbor:"length"`
}

type benchReadAtReq struct {
	HandleID uint64 `cbor:"handle_id"`
	Offset   int64  `cbor:"offset"`
	Length   int    `cbor:"length"`
}

type benchCloseReq struct {
	HandleID uint64 `cbor:"handle_id"`
}

type benchHandleIDResp struct {
	HandleID uint64 `cbor:"handle_id"`
}

// -----------------------------------------------------------------------
// Handle store
// -----------------------------------------------------------------------

type benchHandleStore struct {
	mu      sync.Mutex
	counter uint64
	handles map[uint64]*benchHandle
}

type benchHandle struct {
	rc   io.ReadSeeker
	size int64
	mu   sync.Mutex
}

func newBenchHandleStore() *benchHandleStore {
	return &benchHandleStore{handles: make(map[uint64]*benchHandle)}
}

func (s *benchHandleStore) register(rc io.ReadSeeker, size int64) uint64 {
	s.mu.Lock()
	id := s.counter + 1
	s.counter = id
	s.handles[id] = &benchHandle{rc: rc, size: size}
	s.mu.Unlock()
	return id
}

func (s *benchHandleStore) get(id uint64) (*benchHandle, bool) {
	s.mu.Lock()
	h, ok := s.handles[id]
	s.mu.Unlock()
	return h, ok
}

func (s *benchHandleStore) remove(id uint64) {
	s.mu.Lock()
	delete(s.handles, id)
	s.mu.Unlock()
}

func readFromHandle(h *benchHandle, offset, length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if int64(offset) >= h.size {
		return nil, nil
	}
	remaining := h.size - int64(offset)
	if int64(length) > remaining {
		length = int(remaining)
	}
	if _, err := h.rc.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(h.rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// -----------------------------------------------------------------------
// TLS + TCP arpc setup
// -----------------------------------------------------------------------

func benchNewCA(b *testing.B) *testPKI {
	b.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	ski := make([]byte, 20)
	_, _ = rand.Read(ski)
	caTpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "bench-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
		SubjectKeyId:          ski,
		SignatureAlgorithm:    x509.SHA256WithRSA,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		b.Fatal(err)
	}
	caLeaf, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(caLeaf)
	return &testPKI{caCert: caLeaf, caKey: caKey, caDER: caDER, caPool: pool}
}

func (p *testPKI) benchCert(b *testing.B, cn string, isClient bool, ips []net.IP, dns []string) tls.Certificate {
	b.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	tpl := &x509.Certificate{
		SerialNumber:       serial,
		Subject:            pkix.Name{CommonName: cn},
		NotBefore:          time.Now().Add(-time.Hour),
		NotAfter:           time.Now().Add(48 * time.Hour),
		SignatureAlgorithm: x509.SHA256WithRSA,
		DNSNames:           dns,
		IPAddresses:        ips,
	}
	if isClient {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		b.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{certDER, p.caDER}, PrivateKey: key}
}

func benchSetupPipe(b *testing.B, router Router) *StreamPipe {
	b.Helper()

	srvCA := benchNewCA(b)
	cliCA := benchNewCA(b)

	srvCert := srvCA.benchCert(b, "localhost", false, []net.IP{net.ParseIP("127.0.0.1")}, []string{"localhost"})
	cliCert := cliCA.benchCert(b, "client", true, nil, nil)

	srvTLS := &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    cliCA.caPool,
		MinVersion:   tls.VersionTLS13,
	}

	listener, err := Listen(context.Background(), "127.0.0.1:0", srvTLS)
	if err != nil {
		b.Fatal(err)
	}

	am := NewAgentsManager()
	am.SetExtraExpectFunc(func(string, []*x509.Certificate) bool { return true })

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		_ = Serve(context.Background(), am, listener, router)
	}()
	b.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-srvDone:
		case <-time.After(time.Second):
		}
	})

	cliTLS := &tls.Config{
		Certificates: []tls.Certificate{cliCert},
		RootCAs:      srvCA.caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}

	pipe, err := ConnectToServer(context.Background(), listener.Addr().String(), nil, cliTLS)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { pipe.Close() })
	return pipe
}

// -----------------------------------------------------------------------
// Benchmark: SEPARATE Open + ReadAt
// -----------------------------------------------------------------------

func BenchmarkFileRestore_Separate(b *testing.B) {
	store := newBenchHandleStore()

	router := NewRouter()
	router.Handle("bench.OpenContent", func(req *Request) (Response, error) {
		var params benchOpenReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		data := bytes.Repeat([]byte("A"), int(params.FileSize))
		rc := bytes.NewReader(data)
		handleID := store.register(rc, int64(params.FileSize))
		respData, _ := cbor.Marshal(benchOpenResp{HandleID: handleID})
		return Response{Status: 200, Data: respData}, nil
	})
	router.Handle("bench.ReadContentAt", func(req *Request) (Response, error) {
		var params benchReadAtReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		h, ok := store.get(params.HandleID)
		if !ok {
			return Response{}, fmt.Errorf("handle not found")
		}
		chunk, err := readFromHandle(h, int(params.Offset), params.Length)
		if err != nil {
			return Response{}, err
		}
		return Response{Status: 213, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), len(chunk), stream)
		}}, nil
	})
	router.Handle("bench.CloseContent", func(req *Request) (Response, error) {
		var params benchCloseReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		store.remove(params.HandleID)
		return Response{Status: 200}, nil
	})

	pipe := benchSetupPipe(b, router)
	ctx := context.Background()

	fileSizes := []int64{
		1 << 10, 4 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20,
	}

	buf := make([]byte, benchChunkSize)
	openReq := benchOpenReq{}
	readReq := benchReadAtReq{}
	closeReq := benchCloseReq{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fs := fileSizes[i%len(fileSizes)]

		openReq.FileSize = uint64(fs)
		openResp := benchOpenResp{}
		if err := pipe.Call(ctx, "bench.OpenContent", &openReq, &openResp); err != nil {
			b.Fatal(err)
		}

		readReq.HandleID = openResp.HandleID
		var offset int64
		for offset < fs {
			reqLen := benchChunkSize
			if offset+int64(reqLen) > fs {
				reqLen = int(fs - offset)
			}
			readReq.Offset = offset
			readReq.Length = reqLen
			n, err := pipe.CallBinary(ctx, "bench.ReadContentAt", &readReq, buf)
			if err != nil {
				b.Fatal(err)
			}
			offset += int64(n)
			if n == 0 {
				break
			}
		}

		closeReq.HandleID = openResp.HandleID
		_ = pipe.Call(ctx, "bench.CloseContent", &closeReq, nil)
	}
}

// -----------------------------------------------------------------------
// Benchmark: MERGED ReadContent
// -----------------------------------------------------------------------

func BenchmarkFileRestore_Merged(b *testing.B) {
	store := newBenchHandleStore()

	router := NewRouter()
	router.Handle("bench.ReadContent", func(req *Request) (Response, error) {
		var params benchReadContentReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		data := bytes.Repeat([]byte("A"), int(params.FileSize))
		rc := bytes.NewReader(data)

		reqLen := params.Length
		if int64(reqLen) > int64(params.FileSize) {
			reqLen = int(params.FileSize)
		}

		chunk := make([]byte, reqLen)
		n, _ := io.ReadFull(rc, chunk)
		if n < len(chunk) {
			chunk = chunk[:n]
		}

		var handleID uint64
		if int64(n) < int64(params.FileSize) {
			handleID = store.register(rc, int64(params.FileSize))
		}

		respData, _ := cbor.Marshal(benchHandleIDResp{HandleID: handleID})
		return Response{Status: 213, Data: respData, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), n, stream)
		}}, nil
	})
	router.Handle("bench.ReadContentAt", func(req *Request) (Response, error) {
		var params benchReadAtReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		h, ok := store.get(params.HandleID)
		if !ok {
			return Response{}, fmt.Errorf("handle not found")
		}
		chunk, err := readFromHandle(h, int(params.Offset), params.Length)
		if err != nil {
			return Response{}, err
		}
		return Response{Status: 213, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), len(chunk), stream)
		}}, nil
	})
	router.Handle("bench.CloseContent", func(req *Request) (Response, error) {
		var params benchCloseReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		store.remove(params.HandleID)
		return Response{Status: 200}, nil
	})

	pipe := benchSetupPipe(b, router)
	ctx := context.Background()

	fileSizes := []int64{
		1 << 10, 4 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20,
	}

	buf := make([]byte, benchChunkSize)
	readContentReq := benchReadContentReq{}
	readAtReq := benchReadAtReq{}
	closeReq := benchCloseReq{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fs := fileSizes[i%len(fileSizes)]

		reqLen := benchChunkSize
		if int64(reqLen) > fs {
			reqLen = int(fs)
		}

		readContentReq.FileSize = uint64(fs)
		readContentReq.Length = reqLen
		n, resp, err := pipe.CallBinaryWithMeta(ctx, "bench.ReadContent", &readContentReq, buf)
		if err != nil {
			b.Fatal(err)
		}

		offset := int64(n)
		if offset >= fs {
			continue
		}

		var handleResp benchHandleIDResp
		if err := cbor.Unmarshal(resp.Data, &handleResp); err != nil {
			b.Fatal(err)
		}

		readAtReq.HandleID = handleResp.HandleID
		for offset < fs {
			reqLen := benchChunkSize
			if offset+int64(reqLen) > fs {
				reqLen = int(fs - offset)
			}
			readAtReq.Offset = offset
			readAtReq.Length = reqLen
			n, err := pipe.CallBinary(ctx, "bench.ReadContentAt", &readAtReq, buf)
			if err != nil {
				b.Fatal(err)
			}
			offset += int64(n)
			if n == 0 {
				break
			}
		}

		closeReq.HandleID = handleResp.HandleID
		_ = pipe.Call(ctx, "bench.CloseContent", &closeReq, nil)
	}
}

// -----------------------------------------------------------------------
// Per-file-size sub-benchmarks
// -----------------------------------------------------------------------

func benchFileSizeSeparate(b *testing.B, fileSize int64) {
	store := newBenchHandleStore()

	router := NewRouter()
	router.Handle("bench.OpenContent", func(req *Request) (Response, error) {
		var params benchOpenReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		data := bytes.Repeat([]byte("A"), int(params.FileSize))
		rc := bytes.NewReader(data)
		handleID := store.register(rc, int64(params.FileSize))
		respData, _ := cbor.Marshal(benchOpenResp{HandleID: handleID})
		return Response{Status: 200, Data: respData}, nil
	})
	router.Handle("bench.ReadContentAt", func(req *Request) (Response, error) {
		var params benchReadAtReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		h, ok := store.get(params.HandleID)
		if !ok {
			return Response{}, fmt.Errorf("handle not found")
		}
		chunk, err := readFromHandle(h, int(params.Offset), params.Length)
		if err != nil {
			return Response{}, err
		}
		return Response{Status: 213, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), len(chunk), stream)
		}}, nil
	})
	router.Handle("bench.CloseContent", func(req *Request) (Response, error) {
		var params benchCloseReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		store.remove(params.HandleID)
		return Response{Status: 200}, nil
	})

	pipe := benchSetupPipe(b, router)
	ctx := context.Background()
	buf := make([]byte, benchChunkSize)
	openReq := benchOpenReq{FileSize: uint64(fileSize)}
	readReq := benchReadAtReq{}
	closeReq := benchCloseReq{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		openResp := benchOpenResp{}
		_ = pipe.Call(ctx, "bench.OpenContent", &openReq, &openResp)

		readReq.HandleID = openResp.HandleID
		var offset int64
		for offset < fileSize {
			reqLen := benchChunkSize
			if offset+int64(reqLen) > fileSize {
				reqLen = int(fileSize - offset)
			}
			readReq.Offset = offset
			readReq.Length = reqLen
			n, _ := pipe.CallBinary(ctx, "bench.ReadContentAt", &readReq, buf)
			offset += int64(n)
			if n == 0 {
				break
			}
		}
		closeReq.HandleID = openResp.HandleID
		_ = pipe.Call(ctx, "bench.CloseContent", &closeReq, nil)
	}
}

func benchFileSizeMerged(b *testing.B, fileSize int64) {
	store := newBenchHandleStore()

	router := NewRouter()
	router.Handle("bench.ReadContent", func(req *Request) (Response, error) {
		var params benchReadContentReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		data := bytes.Repeat([]byte("A"), int(params.FileSize))
		rc := bytes.NewReader(data)

		reqLen := params.Length
		if int64(reqLen) > int64(params.FileSize) {
			reqLen = int(params.FileSize)
		}
		chunk := make([]byte, reqLen)
		n, _ := io.ReadFull(rc, chunk)
		if n < len(chunk) {
			chunk = chunk[:n]
		}

		var handleID uint64
		if int64(n) < int64(params.FileSize) {
			handleID = store.register(rc, int64(params.FileSize))
		}

		respData, _ := cbor.Marshal(benchHandleIDResp{HandleID: handleID})
		return Response{Status: 213, Data: respData, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), n, stream)
		}}, nil
	})
	router.Handle("bench.ReadContentAt", func(req *Request) (Response, error) {
		var params benchReadAtReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		h, ok := store.get(params.HandleID)
		if !ok {
			return Response{}, fmt.Errorf("handle not found")
		}
		chunk, err := readFromHandle(h, int(params.Offset), params.Length)
		if err != nil {
			return Response{}, err
		}
		return Response{Status: 213, RawStream: func(stream ARPCStream) {
			_ = SendDataFromReader(bytes.NewReader(chunk), len(chunk), stream)
		}}, nil
	})
	router.Handle("bench.CloseContent", func(req *Request) (Response, error) {
		var params benchCloseReq
		if err := cbor.Unmarshal(req.Payload, &params); err != nil {
			return Response{}, err
		}
		store.remove(params.HandleID)
		return Response{Status: 200}, nil
	})

	pipe := benchSetupPipe(b, router)
	ctx := context.Background()
	buf := make([]byte, benchChunkSize)

	reqLen := benchChunkSize
	if int64(reqLen) > fileSize {
		reqLen = int(fileSize)
	}
	readContentReq := benchReadContentReq{FileSize: uint64(fileSize), Length: reqLen}
	readAtReq := benchReadAtReq{}
	closeReq := benchCloseReq{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		n, resp, _ := pipe.CallBinaryWithMeta(ctx, "bench.ReadContent", &readContentReq, buf)

		offset := int64(n)
		if offset >= fileSize {
			continue
		}

		var handleResp benchHandleIDResp
		_ = cbor.Unmarshal(resp.Data, &handleResp)

		readAtReq.HandleID = handleResp.HandleID
		for offset < fileSize {
			reqLen := benchChunkSize
			if offset+int64(reqLen) > fileSize {
				reqLen = int(fileSize - offset)
			}
			readAtReq.Offset = offset
			readAtReq.Length = reqLen
			rn, _ := pipe.CallBinary(ctx, "bench.ReadContentAt", &readAtReq, buf)
			offset += int64(rn)
			if rn == 0 {
				break
			}
		}
		closeReq.HandleID = handleResp.HandleID
		_ = pipe.Call(ctx, "bench.CloseContent", &closeReq, nil)
	}
}

func BenchmarkSeparate_1KB(b *testing.B)   { benchFileSizeSeparate(b, 1<<10) }
func BenchmarkSeparate_4KB(b *testing.B)   { benchFileSizeSeparate(b, 4<<10) }
func BenchmarkSeparate_64KB(b *testing.B)  { benchFileSizeSeparate(b, 64<<10) }
func BenchmarkSeparate_256KB(b *testing.B) { benchFileSizeSeparate(b, 256<<10) }
func BenchmarkSeparate_1MB(b *testing.B)   { benchFileSizeSeparate(b, 1<<20) }
func BenchmarkSeparate_4MB(b *testing.B)   { benchFileSizeSeparate(b, 4<<20) }

func BenchmarkMerged_1KB(b *testing.B)   { benchFileSizeMerged(b, 1<<10) }
func BenchmarkMerged_4KB(b *testing.B)   { benchFileSizeMerged(b, 4<<10) }
func BenchmarkMerged_64KB(b *testing.B)  { benchFileSizeMerged(b, 64<<10) }
func BenchmarkMerged_256KB(b *testing.B) { benchFileSizeMerged(b, 256<<10) }
func BenchmarkMerged_1MB(b *testing.B)   { benchFileSizeMerged(b, 1<<20) }
func BenchmarkMerged_4MB(b *testing.B)   { benchFileSizeMerged(b, 4<<20) }
