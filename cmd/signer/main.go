package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

const (
	usage = `usage:
  signer sign <artifact> <signature-out>
  signer pubkey

env:
  ED25519_SEED_B64  base64-encoded 32-byte Ed25519 seed`
)

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

func mustSeed() ed25519.PrivateKey {
	seedB64 := os.Getenv("ED25519_SEED_B64")
	if seedB64 == "" {
		fatalf("ED25519_SEED_B64 not set")
	}
	seed, err := base64.StdEncoding.DecodeString(seedB64)
	if err != nil || len(seed) != 32 {
		fatalf("invalid ED25519_SEED_B64 (expect base64-encoded 32-byte seed)")
	}
	return ed25519.NewKeyFromSeed(seed)
}

func cmdPubkey() {
	priv := mustSeed()
	pub := priv.Public().(ed25519.PublicKey)
	fmt.Println(base64.StdEncoding.EncodeToString(pub))
}

func cmdSign(artifact, sigOut string) {
	priv := mustSeed()

	f, err := os.Open(artifact)
	if err != nil {
		fatalf("open artifact: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		fatalf("read artifact: %v", err)
	}

	sig := ed25519.Sign(priv, data)
	if len(sig) != ed25519.SignatureSize {
		fatalf("unexpected signature size %d", len(sig))
	}

	if err := os.WriteFile(sigOut, sig, 0o644); err != nil {
		fatalf("write signature: %v", err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fatalf(usage)
	}
	switch os.Args[1] {
	case "pubkey":
		if len(os.Args) != 2 {
			fatalf(usage)
		}
		cmdPubkey()
	case "sign":
		if len(os.Args) != 4 {
			fatalf(usage)
		}
		cmdSign(os.Args[2], os.Args[3])
	default:
		fatalf(usage)
	}
}
