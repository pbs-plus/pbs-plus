package binarystream

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

// helper: create a connected pair of smux streams over net.Pipe
func newSmuxPair() (*smux.Stream, *smux.Stream, func(), error) {
	c1, c2 := net.Pipe()

	// smux config
	cfg := smux.DefaultConfig()
	cfg.MaxStreamBuffer = utils.MaxStreamBuffer

	// server side
	serverSession, err := smux.Server(c1, cfg)
	if err != nil {
		return nil, nil, nil, err
	}

	// client side
	clientSession, err := smux.Client(c2, cfg)
	if err != nil {
		return nil, nil, nil, err
	}

	// open a stream from client to server
	clientStream, err := clientSession.OpenStream()
	if err != nil {
		return nil, nil, nil, err
	}

	// accept the stream on server side
	serverStream, err := serverSession.AcceptStream()
	if err != nil {
		return nil, nil, nil, err
	}

	cleanup := func() {
		clientStream.Close()
		serverStream.Close()
		clientSession.Close()
		serverSession.Close()
	}

	return clientStream, serverStream, cleanup, nil
}

// generateTestData creates test data of a given size and type.
func generateTestData(size int, dataType string) []byte {
	data := make([]byte, size)

	switch dataType {
	case "random":
		// Random data (not compressible)
		rand.Read(data)
	case "text":
		// Highly compressible text-like data
		pattern := []byte("The quick brown fox jumps over the lazy dog. ")
		for i := 0; i < len(data); i++ {
			data[i] = pattern[i%len(pattern)]
		}
	case "mixed":
		// Partially compressible
		rand.Read(data)
		for i := 0; i < len(data); i += 4 {
			data[i] = byte(i % 256)
		}
	case "zeros":
		// Extremely compressible (all zeros)
		// Already zero-initialized
	}
	return data
}

func BenchmarkSendReceive(b *testing.B) {
	sizes := []int{1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024}
	dataTypes := []string{"random", "text", "zeros"}

	for _, size := range sizes {
		for _, dataType := range dataTypes {
			testData := generateTestData(size, dataType)

			b.Run(fmt.Sprintf("OLD_%s_%dKB", dataType, size/1024), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					clientStream, serverStream, cleanup, err := newSmuxPair()
					if err != nil {
						b.Fatal(err)
					}

					// send from client to server
					go func() {
						_ = OldSendDataFromReader(bytes.NewReader(testData), len(testData), clientStream)
					}()

					recv, n, err := OldReceiveData(serverStream)
					if err != nil {
						b.Fatal(err)
					}
					// --- DATA VERIFICATION ---
					if n != len(testData) {
						b.Fatalf("OLD: received length %d, expected %d", n, len(testData))
					}
					if !bytes.Equal(recv, testData) {
						b.Fatalf("OLD: data mismatch for %s %dKB", dataType, size/1024)
					}
					// --- END VERIFICATION ---
					cleanup()
				}
			})

			b.Run(fmt.Sprintf("NEW_%s_%dKB", dataType, size/1024), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					clientStream, serverStream, cleanup, err := newSmuxPair()
					if err != nil {
						b.Fatal(err)
					}

					// send from client to server
					go func() {
						_ = SendDataFromReader(bytes.NewReader(testData), len(testData), clientStream)
					}()

					recv, n, err := ReceiveData(serverStream)
					if err != nil {
						b.Fatal(err)
					}
					// --- DATA VERIFICATION ---
					if n != len(testData) {
						b.Fatalf("NEW: received length %d, expected %d", n, len(testData))
					}
					if !bytes.Equal(recv, testData) {
						b.Fatalf("NEW: data mismatch for %s %dKB", dataType, size/1024)
					}
					// --- END VERIFICATION ---
					cleanup()
				}
			})

		}
	}
}
