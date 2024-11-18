package utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"

	"golang.org/x/crypto/ssh"
)

func GenerateKeyPair(bitSize int) ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bitSize)
	if err != nil {
		return nil, nil, fmt.Errorf("GenerateKey: error generating RSA key -> %w", err)
	}

	err = privateKey.Validate()
	if err != nil {
		return nil, nil, fmt.Errorf("GenerateKey: error validating private key -> %w", err)
	}

	publicKey, err := generatePublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("GenerateKey: error encoding to byte public key -> %w", err)
	}

	encoded := encodePrivateKeyToPEM(privateKey)

	return encoded, publicKey, nil
}

func encodePrivateKeyToPEM(privateKey *rsa.PrivateKey) []byte {
	privDER := x509.MarshalPKCS1PrivateKey(privateKey)

	privBlock := pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   privDER,
	}

	privatePEM := pem.EncodeToMemory(&privBlock)

	return privatePEM
}

func generatePublicKey(privatekey *rsa.PublicKey) ([]byte, error) {
	publicRsaKey, err := ssh.NewPublicKey(privatekey)
	if err != nil {
		return nil, fmt.Errorf("generatePublicKey: error creating new public key from private key -> %w", err)
	}

	pubKeyBytes := ssh.MarshalAuthorizedKey(publicRsaKey)

	return pubKeyBytes, nil
}

var pubKeyCache sync.Map

func GeneratePublicKeyFromPrivateKey(encodedPrivateKey []byte) ([]byte, error) {
	cached, ok := pubKeyCache.Load(encodedPrivateKey)
	if ok {
		return cached.([]byte), nil
	}

	block, _ := pem.Decode(encodedPrivateKey)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("GeneratePublicKeyFromPrivateKey: invalid private key type or format")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("GeneratePublicKeyFromPrivateKey: error parsing private key -> %w", err)
	}

	publicKey, err := generatePublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("GeneratePublicKeyFromPrivateKey: error generating public key -> %w", err)
	}

	pubKeyCache.Store(encodedPrivateKey, publicKey)
	return publicKey, nil
}
