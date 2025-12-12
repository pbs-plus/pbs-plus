package agentfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	mathRand "math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPKI struct {
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
	caDER  []byte
	caPool *x509.CertPool
}

func newTestCA(t *testing.T, cn string) *testPKI {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	ski := make([]byte, 20)
	_, _ = rand.Read(ski)
	caTpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
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
		t.Fatalf("create ca cert: %v", err)
	}
	caLeaf, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caLeaf)
	return &testPKI{
		caCert: caLeaf,
		caKey:  caKey,
		caDER:  caDER,
		caPool: pool,
	}
}

func (p *testPKI) issueCert(t *testing.T, cn string, isClient bool, ips []net.IP, dns []string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	ski := make([]byte, 20)
	_, _ = rand.Read(ski)
	tpl := &x509.Certificate{
		SerialNumber:       serial,
		Subject:            pkix.Name{CommonName: cn},
		NotBefore:          time.Now().Add(-time.Hour),
		NotAfter:           time.Now().Add(24 * time.Hour),
		KeyUsage:           x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		SignatureAlgorithm: x509.SHA256WithRSA,
		SubjectKeyId:       ski,
	}
	if isClient {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if len(ips) > 0 {
		tpl.IPAddresses = append([]net.IP(nil), ips...)
	}
	if len(dns) > 0 {
		tpl.DNSNames = append([]string(nil), dns...)
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatalf("sign cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der, p.caDER},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

var (
	globalCAOnce sync.Once
	serverCA     *testPKI
	clientCA     *testPKI
)

func ensureGlobalCAs(t *testing.T) {
	globalCAOnce.Do(func() {
		serverCA = newTestCA(t, "agentfs-server-ca")
		clientCA = newTestCA(t, "agentfs-client-ca")
	})
}

func newTestQUICServer(t *testing.T, router arpc.Router) (addr string, cleanup func(), serverTLS *tls.Config) {
	t.Helper()

	ensureGlobalCAs(t)
	serverCert := serverCA.issueCert(t, "localhost", false, []net.IP{net.ParseIP("127.0.0.1")}, []string{"localhost"})

	serverTLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA.caPool,
		NextProtos:   []string{"h2", "http/1.1", "pbsarpc"},
		MinVersion:   tls.VersionTLS13,
	}

	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve udp: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}

	listener, err := quic.Listen(udpConn, serverTLS, &quic.Config{
		KeepAlivePeriod:    200 * time.Millisecond,
		MaxIncomingStreams: quicvarint.Max,
	})
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			go func(c *quic.Conn) {
				defer func() { _ = c.CloseWithError(0, "server shutdown") }()
				pipe, err := arpc.NewStreamPipe(c)
				if err != nil {
					t.Errorf("NewStreamPipe for accepted connection: %v", err)
					return
				}
				pipe.SetRouter(router)
				_ = pipe.Serve()
			}(conn)
		}
	}()

	addr = udpConn.LocalAddr().String()
	cleanup = func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}
	}
	return addr, cleanup, serverTLS
}

func newTestClientTLS(t *testing.T) *tls.Config {
	t.Helper()
	ensureGlobalCAs(t)
	clientCert := clientCA.issueCert(t, "client", true, nil, nil)
	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      serverCA.caPool,
		ServerName:   "localhost",
		NextProtos:   []string{"h2", "http/1.1", "pbsarpc"},
		MinVersion:   tls.VersionTLS13,
	}
}

type latencyConn struct {
	net.Conn
	delay time.Duration
}

func (l *latencyConn) randomDelay() {
	jitter := time.Duration(mathRand.Int63n(int64(l.delay)))
	time.Sleep(l.delay + jitter)
}

func (l *latencyConn) Read(b []byte) (n int, err error) {
	l.randomDelay()
	return l.Conn.Read(b)
}

func (l *latencyConn) Write(b []byte) (n int, err error) {
	l.randomDelay()
	return l.Conn.Write(b)
}

func createLargeTestFile(t *testing.T, path string, size int) {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	const bufferSize = 64 * 1024
	buffer := make([]byte, bufferSize)

	for i := 0; i < bufferSize; i++ {
		buffer[i] = byte(i % 251)
	}

	bytesWritten := 0
	for bytesWritten < size {
		writeSize := bufferSize
		if bytesWritten+writeSize > size {
			writeSize = size - bytesWritten
		}

		n, err := file.Write(buffer[:writeSize])
		require.NoError(t, err)
		require.Equal(t, writeSize, n)

		bytesWritten += writeSize
	}

	require.NoError(t, file.Sync())
}

func createSparseFileWithFsutil(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	cmd := exec.Command("fsutil", "sparse", "setflag", filePath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to mark file as sparse: %w", err)
	}

	_, err = file.WriteAt([]byte("data1"), 0)
	if err != nil {
		return fmt.Errorf("failed to write data region 1: %w", err)
	}

	_, err = file.WriteAt([]byte("data2"), 1048576)
	if err != nil {
		return fmt.Errorf("failed to write data region 2: %w", err)
	}

	_, err = file.WriteAt([]byte("data3"), 3145728)
	if err != nil {
		return fmt.Errorf("failed to write data region 3: %w", err)
	}

	return nil
}

func dumpHandleMap(server *AgentFSServer) string {
	if server == nil || server.handles == nil {
		return "Server or handles map is nil"
	}

	var info strings.Builder
	info.WriteString(fmt.Sprintf("Current handles map contains %d entries:\n", server.handles.Len()))

	server.handles.ForEach(func(key uint64, fh *FileHandle) bool {
		info.WriteString(fmt.Sprintf("  - Handle ID: %d, IsDir: %v\n", key, fh.isDir))
		return true
	})

	return info.String()
}

func TestAgentFSServer(t *testing.T) {
	testDir, err := os.MkdirTemp("", "agentfs-test")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	testFile1Path := filepath.Join(testDir, "test1.txt")
	err = os.WriteFile(testFile1Path, []byte("test file 1 content"), 0644)
	require.NoError(t, err)

	testFile2Path := filepath.Join(testDir, "test2.txt")
	err = os.WriteFile(testFile2Path, []byte("test file 2 content with more data"), 0644)
	require.NoError(t, err)

	largePath := filepath.Join(testDir, "large_file.bin")
	createLargeTestFile(t, largePath, 1024*1024)

	mediumPath := filepath.Join(testDir, "medium_file.bin")
	createLargeTestFile(t, mediumPath, 100*1024)

	subDir := filepath.Join(testDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	require.NoError(t, err)

	subFilePath := filepath.Join(subDir, "subfile.txt")
	err = os.WriteFile(subFilePath, []byte("content in subdirectory"), 0644)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	serverRouter := arpc.NewRouter()
	agentFsServer := NewAgentFSServer("agentFs", "standard", snapshots.Snapshot{Path: testDir, SourcePath: ""})
	agentFsServer.RegisterHandlers(&serverRouter)

	addr, shutdownServer, _ := newTestQUICServer(t, serverRouter)
	defer shutdownServer()

	clientTLS := newTestClientTLS(t)
	clientPipe, err := arpc.ConnectToServer(ctx, addr, nil, clientTLS)
	require.NoError(t, err, "ConnectToServer failed")
	defer clientPipe.Close()

	t.Run("Stat", func(t *testing.T) {
		payload := types.StatReq{Path: "test1.txt"}
		var result types.AgentFileInfo
		err := clientPipe.Call(ctx, "agentFs/Attr", &payload, &result)
		assert.NoError(t, err)
		assert.NotNil(t, result.Size)
		assert.EqualValues(t, 19, result.Size)
	})

	t.Run("Xattr", func(t *testing.T) {
		testFilePath := filepath.Join(testDir, "xattr_test_file.txt")
		err := os.WriteFile(testFilePath, []byte("test content for xattr"), 0644)
		require.NoError(t, err, "Failed to create test file for xattr")

		payload := types.StatReq{Path: "xattr_test_file.txt"}
		var result types.AgentFileInfo
		err = clientPipe.Call(ctx, "agentFs/Xattr", &payload, &result)
		require.NoError(t, err, "Failed to call xattr handler")

		t.Logf("Owner for %s: %+v", testFilePath, result.Owner)
		assert.NotEmpty(t, result.Owner, "Owner should not be empty")
		t.Logf("Group for %s: %+v", testFilePath, result.Group)
		assert.NotEmpty(t, result.Group, "Group should not be empty")
		t.Logf("CreationTime for %s: %+v", testFilePath, result.CreationTime)
		t.Logf("LastAccessTime for %s: %+v", testFilePath, result.LastAccessTime)
		assert.NotEmpty(t, result.LastAccessTime, "LastAccessTime should not be empty")
		t.Logf("LastWriteTime for %s: %+v", testFilePath, result.LastWriteTime)
		assert.NotEmpty(t, result.LastWriteTime, "LastWriteTime should not be empty")
		t.Logf("WinACLs for %s: %+v", testFilePath, result.WinACLs)
		t.Logf("PosixACLs for %s: %+v", testFilePath, result.PosixACLs)
		assert.NotNil(t, result.FileAttributes, "FileAttributes map should not be nil")

		err = os.Remove(testFilePath)
		require.NoError(t, err, "Failed to remove test file")
	})

	t.Run("ReadDir", func(t *testing.T) {
		openPayload := types.OpenFileReq{Path: "/"}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &openPayload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		t.Logf("Handle ID: %d", openResult)

		payload := types.ReadDirReq{HandleID: openResult}
		var result types.ReadDirEntries
		var readDirBytes bytes.Buffer

		readDirHandler := arpc.RawStreamHandler(func(st *quic.Stream) error {
			buf := make([]byte, 64*1024)
			for {
				n, rerr := binarystream.ReceiveDataInto(st, buf)
				if n > 0 {
					readDirBytes.Write(buf[:n])
				}
				if rerr != nil {
					if errors.Is(rerr, io.EOF) {
						break
					}
					return rerr
				}
			}
			return nil
		})

		err = clientPipe.Call(ctx, "agentFs/ReadDir", &payload, readDirHandler)
		assert.NoError(t, err)

		err = result.Decode(readDirBytes.Bytes())
		assert.NoError(t, err)
		t.Logf("Result Size: %v", len(readDirBytes.Bytes()))
		assert.GreaterOrEqual(t, len(result), 3)

		foundTest1 := false
		foundSubdir := false
		for _, entry := range result {
			name := entry.Name
			if name == "test1.txt" {
				foundTest1 = true
			} else if name == "subdir" {
				foundSubdir = true
				assert.True(t, os.FileMode(entry.Mode).IsDir(), "subdir should be identified as a directory")
			}
		}
		assert.True(t, foundTest1, "test1.txt should be found in directory listing")
		assert.True(t, foundSubdir, "subdir should be found in directory listing")

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		if err != nil {
			t.Logf("Close error: %v - Current handle map: %s", err, dumpHandleMap(agentFsServer))
			t.FailNow()
		}
		assert.NoError(t, err)
	})

	t.Run("OpenFile_ReadAt_Close", func(t *testing.T) {
		t.Log("Before OpenFile:", dumpHandleMap(agentFsServer))

		payload := types.OpenFileReq{Path: "test2.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		t.Logf("After OpenFile - Handle ID received: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		exists := false
		agentFsServer.handles.ForEach(func(key uint64, fh *FileHandle) bool {
			if key == uint64(openResult) {
				exists = true
				return false
			}
			return true
		})
		require.True(t, exists, "Handle ID should exist in server's handles map")

		readAtPayload := types.ReadAtReq{
			HandleID: openResult,
			Offset:   10,
			Length:   100,
		}

		t.Logf("Before ReadAt - Using Handle ID: %d", uint64(readAtPayload.HandleID))
		t.Log(dumpHandleMap(agentFsServer))

		var readAtBytes bytes.Buffer
		readAtHandler := arpc.RawStreamHandler(func(st *quic.Stream) error {
			buf := make([]byte, 32*1024)
			for {
				n, rerr := binarystream.ReceiveDataInto(st, buf)
				if n > 0 {
					readAtBytes.Write(buf[:n])
				}
				if rerr != nil {
					if errors.Is(rerr, io.EOF) {
						break
					}
					return rerr
				}
			}
			return nil
		})

		err = clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload, readAtHandler)
		if err != nil {
			t.Logf("ReadAt error: %v - Current handle map: %s", err, dumpHandleMap(agentFsServer))
			t.FailNow()
		}

		assert.Equal(t, "2 content with more data", readAtBytes.String())

		t.Logf("Before Close - Using Handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		if err != nil {
			t.Logf("Close error: %v - Current handle map: %s", err, dumpHandleMap(agentFsServer))
			t.FailNow()
		}
		assert.NoError(t, err)

		t.Log("After Close:", dumpHandleMap(agentFsServer))
	})

	t.Run("MultipleFiles_HandleManagement", func(t *testing.T) {
		t.Log("Initial handle map:", dumpHandleMap(agentFsServer))

		handles := make([]types.FileHandleId, 0, 5)

		files := []string{"test1.txt", "test2.txt", "large_file.bin", "medium_file.bin", "subdir/subfile.txt"}
		for i, fileName := range files {
			t.Logf("Opening file %d: %s", i, fileName)

			payload := types.OpenFileReq{Path: fileName, Flag: 0, Perm: 0644}
			var openResult types.FileHandleId
			err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
			require.NoError(t, err, "OpenFile should succeed for %s", fileName)

			t.Logf("Received handle ID: %d for file: %s", uint64(openResult), fileName)
			handles = append(handles, openResult)

			t.Log(dumpHandleMap(agentFsServer))
		}

		for i, handle := range handles {
			t.Logf("Reading from file %d with handle: %d", i, uint64(handle))

			readSize := 10
			readAtPayload := types.ReadAtReq{
				HandleID: handle,
				Offset:   0,
				Length:   readSize,
			}

			var readAtBytes bytes.Buffer
			readAtHandler := arpc.RawStreamHandler(func(st *quic.Stream) error {
				buf := make([]byte, readSize)
				n, err := binarystream.ReceiveDataInto(st, buf)
				if err != nil && !errors.Is(err, io.EOF) {
					return fmt.Errorf("read from stream failed: %w", err)
				}
				readAtBytes.Write(buf[:n])
				return nil
			})

			err = clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload, readAtHandler)
			if err != nil {
				t.Logf("ReadAt error for handle %d: %v - Current handle map: %s",
					uint64(handle), err, dumpHandleMap(agentFsServer))
				t.FailNow()
			}
		}

		for i, handle := range handles {
			t.Logf("Closing file %d with handle: %d", i, uint64(handle))

			closePayload := types.CloseReq{HandleID: handle}

			t.Log("Before Close:", dumpHandleMap(agentFsServer))
			err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
			if err != nil {
				t.Logf("Close error for handle %d: %v - Current handle map: %s",
					uint64(handle), err, dumpHandleMap(agentFsServer))
				t.FailNow()
			}
			assert.NoError(t, err)
			t.Log("After Close:", dumpHandleMap(agentFsServer))
		}
	})

	t.Run("LargeFile_Read", func(t *testing.T) {
		payload := types.OpenFileReq{Path: "large_file.bin", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		assert.NoError(t, err)

		t.Logf("Large file open, handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		readSize := 256 * 1024
		readAtPayload := types.ReadAtReq{
			HandleID: openResult,
			Offset:   1024,
			Length:   readSize,
		}

		var receivedLargeFileBytes bytes.Buffer
		readAtHandler := arpc.RawStreamHandler(func(st *quic.Stream) error {
			buf := make([]byte, 256*1024)
			total := 0
			for total < readSize {
				n, rerr := binarystream.ReceiveDataInto(st, buf)
				if n > 0 {
					receivedLargeFileBytes.Write(buf[:n])
					total += n
				}
				if rerr != nil {
					if errors.Is(rerr, io.EOF) {
						break
					}
					return rerr
				}
			}
			return nil
		})

		err = clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload, readAtHandler)
		if err != nil {
			t.Logf("Large file ReadAt error: %v - Current handle map: %s", err, dumpHandleMap(agentFsServer))
			t.FailNow()
		}
		assert.Equal(t, readSize, receivedLargeFileBytes.Len(), "Should read the full requested size")

		originalFile, err := os.Open(largePath)
		require.NoError(t, err)
		defer originalFile.Close()

		_, err = originalFile.Seek(1024, 0)
		require.NoError(t, err)

		compareBuffer := make([]byte, 1024)
		_, err = io.ReadFull(originalFile, compareBuffer)
		require.NoError(t, err)

		assert.Equal(t, compareBuffer, receivedLargeFileBytes.Bytes()[:1024], "First 1KB of read data should match original file")

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		if err != nil {
			t.Logf("Large file Close error: %v - Current handle map: %s", err, dumpHandleMap(agentFsServer))
			t.FailNow()
		}
		assert.NoError(t, err)
	})

	t.Run("InvalidHandle_Operations", func(t *testing.T) {
		readAtPayload := types.ReadAtReq{
			HandleID: 33123,
			Offset:   0,
			Length:   100,
		}

		t.Log("Current handle map before invalid ReadAt:", dumpHandleMap(agentFsServer))
		err := clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload, arpc.RawStreamHandler(func(st *quic.Stream) error { return nil }))
		assert.Error(t, err, "ReadAt with invalid handle should return an error")

		closePayload := types.CloseReq{HandleID: 33123}

		t.Log("Current handle map before invalid Close:", dumpHandleMap(agentFsServer))
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.Error(t, err, "Close with invalid handle should return an error")
	})

	t.Run("DoubleClose", func(t *testing.T) {
		payload := types.OpenFileReq{Path: "test1.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err)

		t.Logf("File opened with handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.NoError(t, err)

		t.Log("After first close:", dumpHandleMap(agentFsServer))

		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.Error(t, err, "Second close with same handle should return an error")

		t.Log("After second close:", dumpHandleMap(agentFsServer))
	})

	t.Run("Lseek", func(t *testing.T) {
		payload := types.OpenFileReq{Path: "test2.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		t.Logf("File opened with handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		t.Run("SeekStart", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   0,
				Whence:   io.SeekStart,
			}

			var lseekResp types.LseekResp
			err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
			require.NoError(t, err, "Lseek should succeed")

			assert.Equal(t, int64(0), lseekResp.NewOffset, "Offset should be at the start of the file")
		})

		t.Run("SeekMiddle", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   10,
				Whence:   io.SeekStart,
			}

			var lseekResp types.LseekResp
			err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
			require.NoError(t, err, "Lseek should succeed")

			assert.Equal(t, int64(10), lseekResp.NewOffset, "Offset should be at position 10")
		})

		t.Run("SeekCurrent", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   5,
				Whence:   io.SeekCurrent,
			}

			var lseekResp types.LseekResp
			err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
			require.NoError(t, err, "Lseek should succeed")

			assert.Equal(t, int64(15), lseekResp.NewOffset, "Offset should be at position 15")
		})

		t.Run("SeekEnd", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   -5,
				Whence:   io.SeekEnd,
			}

			var lseekResp types.LseekResp
			err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
			require.NoError(t, err, "Lseek should succeed")

			t.Logf("File size: %d", 34)
			t.Logf("Expected offset: %d", 34-5)
			t.Logf("Actual offset: %d", lseekResp.NewOffset)

			assert.Equal(t, int64(29), lseekResp.NewOffset, "Offset should be 5 bytes before the end of the file")
		})

		t.Run("SeekBeyondEOF", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   100,
				Whence:   io.SeekStart,
			}

			err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, nil)
			require.Error(t, err, "Lseek should fail when seeking beyond EOF")
		})

		if runtime.GOOS == "windows" {
			t.Run("SeekSparseFile", func(t *testing.T) {
				sparseFilePath := filepath.Join(testDir, "sparse_file.bin")
				err := createSparseFileWithFsutil(sparseFilePath)
				require.NoError(t, err, "Failed to create sparse file with fsutil")

				payload := types.OpenFileReq{Path: "sparse_file.bin", Flag: 0, Perm: 0644}
				var openResult types.FileHandleId
				err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
				require.NoError(t, err, "OpenFile should succeed for sparse file")

				t.Logf("Sparse file opened with handle ID: %d", uint64(openResult))
				t.Log(dumpHandleMap(agentFsServer))

				t.Run("SeekData", func(t *testing.T) {
					lseekPayload := types.LseekReq{
						HandleID: openResult,
						Offset:   0,
						Whence:   SeekData,
					}

					var lseekResp types.LseekResp
					err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
					require.NoError(t, err, "SeekData should succeed")

					t.Logf("SeekData returned offset: %d", lseekResp.NewOffset)
					assert.Equal(t, int64(0), lseekResp.NewOffset, "SeekData should return the start of the first data region")

					lseekPayload.Offset = 1024 * 1024

					err = clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
					require.NoError(t, err, "SeekData should succeed")
					t.Logf("SeekData returned offset: %d", lseekResp.NewOffset)
					assert.Equal(t, int64(1024*1024), lseekResp.NewOffset, "SeekData should return the start of the second data region")
				})

				t.Run("SeekHole", func(t *testing.T) {
					lseekPayload := types.LseekReq{
						HandleID: openResult,
						Offset:   0,
						Whence:   SeekHole,
					}

					var lseekResp types.LseekResp
					err := clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
					require.NoError(t, err, "SeekHole should succeed")

					t.Logf("SeekHole returned offset: %d", lseekResp.NewOffset)
					assert.Equal(t, int64(65536), lseekResp.NewOffset, "SeekHole should return the start of the first hole region")

					lseekPayload.Offset = 1048576
					err = clientPipe.Call(ctx, "agentFs/Lseek", &lseekPayload, &lseekResp)
					require.NoError(t, err, "SeekHole should succeed")

					t.Logf("SeekHole returned offset: %d", lseekResp.NewOffset)
					assert.Equal(t, int64(1114112), lseekResp.NewOffset, "SeekHole should return the start of the second hole region")
				})

				closePayload := types.CloseReq{HandleID: openResult}
				err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
				assert.NoError(t, err, "Close should succeed")
			})
		}

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.NoError(t, err, "Close should succeed")
	})

	t.Run("ConcurrentReadAt", func(t *testing.T) {
		payload := types.OpenFileReq{Path: "test2.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		t.Logf("File opened with handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(agentFsServer))

		const numGoroutines = 10
		const readSize = 10
		results := make([]string, numGoroutines)
		errorsList := make([]error, numGoroutines)

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				offset := int64(goroutineID * readSize)
				readAtPayload := types.ReadAtReq{
					HandleID: openResult,
					Offset:   offset,
					Length:   readSize,
				}

				var goroutineReadBytes bytes.Buffer
				readAtHandler := arpc.RawStreamHandler(func(st *quic.Stream) error {
					buf := make([]byte, readSize)
					n, copyErr := binarystream.ReceiveDataInto(st, buf)
					if copyErr != nil && !errors.Is(copyErr, io.EOF) {
						return fmt.Errorf("read from stream failed: %w", copyErr)
					}
					goroutineReadBytes.Write(buf[:n])
					return nil
				})

				err := clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload, readAtHandler)
				if err != nil {
					errorsList[goroutineID] = err
					return
				}

				results[goroutineID] = goroutineReadBytes.String()
			}(i)
		}

		wg.Wait()

		for i, err := range errorsList {
			assert.NoError(t, err, "Goroutine %d encountered an error", i)
		}

		expectedContent := "test file 2 content with more data"
		for i, result := range results {
			start := i * readSize
			var expected string
			if start >= len(expectedContent) {
				expected = ""
			} else {
				end := start + readSize
				if end > len(expectedContent) {
					end = len(expectedContent)
				}
				expected = expectedContent[start:end]
			}
			t.Logf("Goroutine %d: Expected=%q, Actual=%q", i, expected, result)
			assert.Equal(t, expected, result, "Goroutine %d read incorrect data", i)
		}

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.NoError(t, err, "Close should succeed")
	})

	t.Run("StressTest_HandleManagement", func(t *testing.T) {
		const numFiles = 10
		const numIterations = 10

		for i := 0; i < numFiles; i++ {
			filePath := filepath.Join(testDir, fmt.Sprintf("stress_test_file_%d.txt", i))
			err := os.WriteFile(filePath, []byte(fmt.Sprintf("content for file %d", i)), 0644)
			require.NoError(t, err, "Failed to create test file %d", i)
		}

		for iteration := 0; iteration < numIterations; iteration++ {
			t.Logf("Iteration %d: Opening and closing files", iteration)

			for i := 0; i < numFiles; i++ {
				filePath := fmt.Sprintf("stress_test_file_%d.txt", i)

				payload := types.OpenFileReq{Path: filePath, Flag: 0, Perm: 0644}
				var openResult types.FileHandleId
				err := clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
				require.NoError(t, err, "OpenFile should succeed for %s", filePath)

				closePayload := types.CloseReq{HandleID: openResult}
				err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
				assert.NoError(t, err, "Close should succeed for %s", filePath)
			}
		}

		assert.Equal(t, 0, agentFsServer.handles.Len(), "All handles should be closed after stress test")
	})

	t.Run("ResourceLeakTest", func(t *testing.T) {
		initialHandleCount := agentFsServer.handles.Len()

		payload := types.OpenFileReq{Path: "test1.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.NoError(t, err, "Close should succeed")

		finalHandleCount := agentFsServer.handles.Len()
		assert.Equal(t, initialHandleCount, finalHandleCount, "Handle count should remain unchanged after open/close")
	})

	t.Run("FilePointerIsolation", func(t *testing.T) {
		payload := types.OpenFileReq{Path: "test2.txt", Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		err = clientPipe.Call(ctx, "agentFs/OpenFile", &payload, &openResult)
		require.NoError(t, err, "OpenFile should succeed")

		readAtPayload1 := types.ReadAtReq{
			HandleID: openResult,
			Offset:   0,
			Length:   10,
		}
		readAtPayload2 := types.ReadAtReq{
			HandleID: openResult,
			Offset:   20,
			Length:   10,
		}

		var buffer1 bytes.Buffer
		readAtHandler1 := arpc.RawStreamHandler(func(st *quic.Stream) error {
			buf := make([]byte, readAtPayload1.Length)
			n, copyErr := binarystream.ReceiveDataInto(st, buf)
			if copyErr != nil && !errors.Is(copyErr, io.EOF) {
				return fmt.Errorf("read from stream failed: %w", copyErr)
			}
			buffer1.Write(buf[:n])
			return nil
		})

		var buffer2 bytes.Buffer
		readAtHandler2 := arpc.RawStreamHandler(func(st *quic.Stream) error {
			buf := make([]byte, readAtPayload2.Length)
			n, copyErr := binarystream.ReceiveDataInto(st, buf)
			if copyErr != nil && !errors.Is(copyErr, io.EOF) {
				return fmt.Errorf("read from stream failed: %w", copyErr)
			}
			buffer2.Write(buf[:n])
			return nil
		})

		err1 := clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload1, readAtHandler1)
		err2 := clientPipe.Call(ctx, "agentFs/ReadAt", &readAtPayload2, readAtHandler2)

		assert.NoError(t, err1, "First ReadAt should succeed")
		assert.NoError(t, err2, "Second ReadAt should succeed")

		assert.Equal(t, "test file ", buffer1.String(), "First ReadAt returned incorrect data")
		assert.Equal(t, "with more ", buffer2.String(), "Second ReadAt returned incorrect data")

		closePayload := types.CloseReq{HandleID: openResult}
		err = clientPipe.Call(ctx, "agentFs/Close", &closePayload, nil)
		assert.NoError(t, err, "Close should succeed")
	})
}
