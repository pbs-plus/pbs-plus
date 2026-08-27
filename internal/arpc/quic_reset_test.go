package arpc

import (
	"bytes"
	"testing"
)

func TestStatelessResetKeyIsStableAcrossRestarts(t *testing.T) {
	secret := []byte("server private key material")

	first, err := statelessResetKey(secret)
	if err != nil {
		t.Fatalf("derive first key: %v", err)
	}
	second, err := statelessResetKey(secret)
	if err != nil {
		t.Fatalf("derive second key: %v", err)
	}

	if !bytes.Equal(first[:], second[:]) {
		t.Fatal("same secret produced different keys; a restarted server " +
			"would not reset agents holding dead connections")
	}

	other, err := statelessResetKey([]byte("a different server"))
	if err != nil {
		t.Fatalf("derive other key: %v", err)
	}
	if bytes.Equal(first[:], other[:]) {
		t.Fatal("different secrets produced the same key")
	}

	var zero [32]byte
	if bytes.Equal(first[:], zero[:]) {
		t.Fatal("derived key is all zeroes")
	}
}

func TestNewQuicTransportWithoutSecret(t *testing.T) {
	transport, err := NewQuicTransport("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("bind transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	if transport.StatelessResetKey != nil {
		t.Fatal("expected no stateless reset key without a secret")
	}
}

func TestNewQuicTransportSetsResetKey(t *testing.T) {
	transport, err := NewQuicTransport("127.0.0.1:0", []byte("secret"))
	if err != nil {
		t.Fatalf("bind transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	if transport.StatelessResetKey == nil {
		t.Fatal("expected a stateless reset key to be configured")
	}
}
