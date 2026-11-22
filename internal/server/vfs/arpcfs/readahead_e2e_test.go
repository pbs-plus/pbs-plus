//go:build linux

package arpcfs

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"go.uber.org/goleak"
)

type e2eSession struct {
	fs       *ARPCFS
	agent    *arpc.StreamPipe
	listener net.Listener
	cancel   context.CancelFunc
}

// newE2ESession mirrors production: PBS listens with an AgentsManager, the agent dials in.
func newE2ESession(t *testing.T, router arpc.Router) *e2eSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	serverCA, clientCA := newE2ECA(t, "server-ca"), newE2ECA(t, "client-ca")

	listener, err := arpc.Listen(ctx, "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCA.issue(t, "localhost", false)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA.pool,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	agents := arpc.NewAgentsManager()
	agents.SetExtraExpectFunc(func(string, []*x509.Certificate) bool { return true })
	go func() { _ = arpc.Serve(ctx, agents, listener, arpc.NewRouter()) }()

	agent, err := arpc.ConnectToServer(ctx, listener.Addr().String(), nil, &tls.Config{
		Certificates: []tls.Certificate{clientCA.issue(t, "agent", true)},
		RootCAs:      serverCA.pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		cancel()
		_ = listener.Close()
		t.Fatalf("agent connect: %v", err)
	}
	agent.SetRouter(router)
	go func() { _ = agent.Serve() }()
	waitFor(t, func() bool { return agents.IsOnline("agent") }, "agent session never registered")

	return &e2eSession{
		fs: &ARPCFS{
			VFSBase:      vfs.InjectBase(vfs.VFSBase{Ctx: ctx}),
			agentManager: agents,
			sessionId:    "agent",
			Hostname:     "agent",
		},
		agent:    agent,
		listener: listener,
		cancel:   cancel,
	}
}

func (s *e2eSession) close() {
	s.agent.Close()
	_ = s.listener.Close()
	s.cancel()
}

func (s *e2eSession) open(t *testing.T, name string) *FileHandle {
	t.Helper()
	f, err := s.fs.OpenFile(t.Context(), name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	return newFileHandle(s.fs, f)
}

func openFDsUnder(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("list fds: %v", err)
	}
	n := 0
	for _, e := range entries {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, dir+"/") {
			n++
		}
	}
	return n
}

func goroutinesIn(fn string) int {
	buf := make([]byte, 4<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), fn)
}

func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// Full and abandoned reads must leave no agent fds, handles, goroutines or heap growth.
func TestReadaheadEndToEndLeaksNothing(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	dir := t.TempDir()
	const size = 24<<20 + 12345
	data := testData(size)
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	agentFS := agentfs.NewServer("job", "standard", snapshots.Snapshot{Path: dir})
	router := arpc.NewRouter()
	agentFS.RegisterHandlers(&router)
	s := newE2ESession(t, router)
	var probing atomic.Bool
	var rejected atomic.Int64
	s.fs.remoteReadAt = func(ctx context.Context, h fswire.FileHandleID, p []byte, off int64) (int, error) {
		pipe, ok := s.fs.agentManager.GetStreamPipe(s.fs.sessionId)
		if !ok {
			return 0, ErrSessionLost
		}
		n, err := pipe.CallBinary(ctx, "ReadAt", &fswire.ReadAtReq{HandleID: h, Offset: off, Length: len(p)}, p)
		if err != nil && !probing.Load() {
			rejected.Add(1)
			t.Logf("agent rejected ReadAt handle %d off %d: %v", h, off, err)
		}
		return n, err
	}

	cycle := func(i int) {
		fh := s.open(t, "big.bin")
		limit := int64(size)
		if i%2 == 1 {
			limit = int64(3<<20 + i*kernelReadSize)
		}
		var got []byte
		for off := int64(0); off < limit; {
			r := readVia(t, fh, off, kernelReadSize)
			if r.errno != 0 {
				t.Fatalf("cycle %d read at %d: %v", i, off, r.errno)
			}
			if len(r.data) == 0 {
				break
			}
			got = append(got, r.data...)
			off += int64(len(r.data))
		}
		if !bytes.Equal(got, data[:min(int64(len(got)), int64(size))]) || (i%2 == 0 && len(got) != size) {
			t.Fatalf("cycle %d: read %d bytes that do not match the source", i, len(got))
		}
		handle := fh.file.handleID
		if err := fh.Release(t.Context()); err != 0 {
			t.Fatalf("cycle %d release: %v", i, err)
		}
		probing.Store(true)
		_, err := s.fs.readRemote(t.Context(), handle, make([]byte, 1), 0)
		probing.Store(false)
		if err == nil {
			t.Fatalf("cycle %d: agent still serves handle %d after release", i, handle)
		}
	}

	for i := range 2 {
		cycle(i)
	}
	base := heapInUse()
	for i := 2; i < 64; i++ {
		cycle(i)
	}
	if n := rejected.Load(); n != 0 {
		t.Errorf("agent rejected %d prefetch or kernel reads during healthy cycles", n)
	}
	waitFor(t, func() bool { return openFDsUnder(t, dir) == 0 }, "agent kept source files open after release")
	waitFor(t, func() bool { return goroutinesIn("arpcfs.(*readahead).fetch") == 0 }, "prefetch goroutines outlived release while the session is up")
	waitFor(t, func() bool { return goroutinesIn("agentfs.(*Server).handleReadAt") == 0 }, "agent read handlers outlived release while the session is up")
	if grown := int64(heapInUse()) - int64(base); grown > 16<<20 {
		t.Errorf("heap grew %d MiB over 14 open/read/release cycles", grown>>20)
	}
	if s.fs.sessionDown.Load() {
		t.Error("session marked lost during healthy reads")
	}

	agentFS.Close()
	s.close()
}

// A stalled arpc stream must not block Release or outlive the prefetch deadline.
func TestReadaheadHungAgentOverARPC(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	old := readaheadTimeout
	readaheadTimeout = 300 * time.Millisecond
	defer func() { readaheadTimeout = old }()

	const size = 8 << 20
	data := testData(size)
	unhang := make(chan struct{})
	defer close(unhang)

	router := arpc.NewRouter()
	router.Handle("OpenFile", func(*arpc.Request) (arpc.Response, error) {
		raw, err := cbor.Marshal(fswire.FileHandleID(7))
		return arpc.Response{Status: 200, Data: raw}, err
	})
	router.Handle("Close", func(*arpc.Request) (arpc.Response, error) {
		return arpc.Response{Status: 200}, nil
	})
	router.Handle("ReadAt", func(req *arpc.Request) (arpc.Response, error) {
		var p fswire.ReadAtReq
		if err := cbor.Unmarshal(req.Payload, &p); err != nil {
			return arpc.Response{}, err
		}
		chunk := expected(data, p.Offset, p.Length)
		return arpc.Response{Status: arpc.StatusRawStream, RawStream: func(st arpc.ARPCStream) {
			if p.Offset >= readaheadBlock {
				select {
				case <-unhang:
				case <-time.After(5 * time.Second):
				}
			}
			_ = arpc.SendDataFromReader(bytes.NewReader(chunk), len(chunk), st)
		}}, nil
	})
	s := newE2ESession(t, router)
	defer s.close()

	fh := s.open(t, "stalled.bin")
	for off := int64(0); off < readaheadBlock; off += kernelReadSize {
		if r := readVia(t, fh, off, kernelReadSize); r.errno != 0 || !bytes.Equal(r.data, expected(data, off, kernelReadSize)) {
			t.Fatalf("read at %d: %d bytes errno %v", off, len(r.data), r.errno)
		}
	}
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("release blocked %v behind a stalled prefetch", d)
	}
	if goroutinesIn("arpcfs.(*readahead).fetch") == 0 {
		t.Fatal("no prefetch was stalled; the test did not exercise a hang")
	}
	time.Sleep(readaheadTimeout + 200*time.Millisecond)
	if n := goroutinesIn("arpcfs.(*readahead).fetch"); n != 0 {
		t.Fatalf("%d prefetch goroutines still stalled past the deadline with the session up", n)
	}
	if s.fs.sessionDown.Load() {
		t.Fatal("a stalled prefetch marked the session lost")
	}
}

type e2eCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
	pool *x509.CertPool
}

func newE2ECA(t *testing.T, cn string) *e2eCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &e2eCA{cert: cert, key: key, der: der, pool: pool}
}

func (ca *e2eCA) issue(t *testing.T, cn string, client bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	if client {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der, ca.der}, PrivateKey: key}
}
