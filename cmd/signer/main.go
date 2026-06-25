package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	usage = `usage:
  signer ed25519-pubkey
  signer ed25519-sign <artifact> <signature-out>
  signer ecdsa-pubkey
  signer ecdsa-sign <artifact> <signature-out>

env:
  ED25519_SEED_B64     base64-encoded 32-byte Ed25519 seed
  ECDSA_PRIVATE_KEY    PEM-encoded ECDSA P-256 private key (or path to PEM file)`
)

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

func mustEd25519Seed() ed25519.PrivateKey {
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

func mustECDSAKey() *ecdsa.PrivateKey {
	keyData := os.Getenv("ECDSA_PRIVATE_KEY")
	if keyData == "" {
		fatalf("ECDSA_PRIVATE_KEY not set")
	}

	var pemData []byte
	if _, err := os.Stat(keyData); err == nil {
		pemData, err = os.ReadFile(keyData)
		if err != nil {
			fatalf("read ECDSA key file: %v", err)
		}
	} else {
		pemData = []byte(keyData)
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		fatalf("failed to decode ECDSA private key PEM")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		fatalf("parse ECDSA private key: %v", err)
	}

	ecdsaKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		fatalf("key is not ECDSA (got %T)", key)
	}

	if ecdsaKey.Curve != elliptic.P256() {
		fatalf("key must be ECDSA P-256, got %s", ecdsaKey.Curve.Params().Name)
	}

	return ecdsaKey
}

func cmdEd25519Pubkey() {
	priv := mustEd25519Seed()
	pub := priv.Public().(ed25519.PublicKey)
	fmt.Println(base64.StdEncoding.EncodeToString(pub))
}

func cmdEd25519Sign(artifact, sigOut string) {
	priv := mustEd25519Seed()

	f, err := os.Open(artifact)
	if err != nil {
		fatalf("open artifact: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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

func cmdECDSAPubkey() {
	key := mustECDSAKey()

	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		fatalf("marshal ECDSA public key: %v", err)
	}

	fmt.Println(base64.StdEncoding.EncodeToString(der))
}

func cmdECDSASign(artifact, sigOut string) {
	key := mustECDSAKey()

	f, err := os.Open(artifact)
	if err != nil {
		fatalf("open artifact: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	data, err := io.ReadAll(f)
	if err != nil {
		fatalf("read artifact: %v", err)
	}

	hash := crypto.SHA256(data)
	r, s, err := ecdsa.Sign(rand.Reader, key, hash[:])
	if err != nil {
		fatalf("ECDSA sign: %v", err)
	}

	sig, err := asn1.Marshal(struct {
		R, S *big.Int
	}{R: r, S: s})
	if err != nil {
		fatalf("marshal ECDSA signature: %v", err)
	}

	encoded := base64.StdEncoding.EncodeToString(sig)
	if err := os.WriteFile(sigOut, []byte(encoded), 0o644); err != nil {
		fatalf("write signature: %v", err)
	}
}

func main() {
	if err := crypto.AssertFIPS(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

	if len(os.Args) < 2 {
		fatalf(usage)
	}
	switch os.Args[1] {
	case "ed25519-pubkey":
		if len(os.Args) != 2 {
			fatalf(usage)
		}
		cmdEd25519Pubkey()
	case "ed25519-sign":
		if len(os.Args) != 4 {
			fatalf(usage)
		}
		cmdEd25519Sign(os.Args[2], os.Args[3])
	case "ecdsa-pubkey":
		if len(os.Args) != 2 {
			fatalf(usage)
		}
		cmdECDSAPubkey()
	case "ecdsa-sign":
		if len(os.Args) != 4 {
			fatalf(usage)
		}
		cmdECDSASign(os.Args[2], os.Args[3])
	default:
		fatalf(usage)
	}
}
