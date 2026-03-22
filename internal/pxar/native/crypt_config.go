package native

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

type CryptConfig struct {
	key [32]byte
}

func NewCryptConfig(key [32]byte) (*CryptConfig, error) {
	return &CryptConfig{key: key}, nil
}

func (c *CryptConfig) Key() *[32]byte {
	return &c.key
}

func (c *CryptConfig) ComputeDigest(data []byte) [32]byte {
	return Sha256Sum(data)
}

func (c *CryptConfig) Encrypt(plaintext []byte) (iv [16]byte, tag [16]byte, ciphertext []byte, err error) {
	var gcmNonce [12]byte
	if _, err := io.ReadFull(rand.Reader, gcmNonce[:]); err != nil {
		return iv, tag, nil, fmt.Errorf("generate IV: %w", err)
	}
	copy(iv[:], gcmNonce[:])

	block, err := aes.NewCipher(c.key[:])
	if err != nil {
		return iv, tag, nil, fmt.Errorf("create cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return iv, tag, nil, fmt.Errorf("create GCM: %w", err)
	}

	sealed := aesgcm.Seal(nil, gcmNonce[:], plaintext, nil)
	ciphertext = sealed[:len(plaintext)]
	copy(tag[:], sealed[len(plaintext):])

	return iv, tag, ciphertext, nil
}

func (c *CryptConfig) Decrypt(ciphertext []byte, iv []byte, tag []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	gcmNonce := iv[:12]

	sealed := make([]byte, len(ciphertext)+len(tag))
	copy(sealed, ciphertext)
	copy(sealed[len(ciphertext):], tag)

	plaintext, err := aesgcm.Open(nil, gcmNonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

func (c *CryptConfig) DecryptStreaming(ciphertext []byte, iv []byte, tag []byte) ([]byte, error) {
	return c.Decrypt(ciphertext, iv, tag)
}

func DeriveKey(password []byte, salt []byte) [32]byte {
	h := sha256.New()
	h.Write(password)
	h.Write(salt)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

func LoadEncryptionKey(keyfile string, password []byte) (*CryptConfig, error) {
	keyData, err := ReadEncryptionKeyFile(keyfile, password)
	if err != nil {
		return nil, err
	}
	return NewCryptConfig(keyData)
}

func ReadEncryptionKeyFile(keyfile string, password []byte) ([32]byte, error) {
	data, err := io.ReadAll(openFile(keyfile))
	if err != nil {
		return [32]byte{}, fmt.Errorf("read key file: %w", err)
	}

	return DecryptKeyFile(data, password)
}

func DecryptKeyFile(data []byte, password []byte) ([32]byte, error) {
	if len(data) < 32 {
		return [32]byte{}, fmt.Errorf("key file too small")
	}

	magic := data[0:4]
	if string(magic) != "PBK2" {
		return [32]byte{}, fmt.Errorf("invalid key file magic")
	}

	salt := data[4:20]
	iv := data[20:36]
	tag := data[36:52]
	encryptedKey := data[52:84]

	derivedKey := DeriveKey(password, salt)

	config, err := NewCryptConfig(derivedKey)
	if err != nil {
		return [32]byte{}, err
	}

	key, err := config.Decrypt(encryptedKey, iv, tag)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decrypt key: %w", err)
	}

	var result [32]byte
	copy(result[:], key)
	return result, nil
}

func openFile(path string) io.ReadCloser {
	f, err := readFile(path)
	if err != nil {
		return nil
	}
	return f
}

type fileReader struct {
	data []byte
	pos  int
}

func (r *fileReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *fileReader) Close() error {
	return nil
}

func readFile(path string) (*fileReader, error) {
	return nil, fmt.Errorf("not implemented")
}
