package native

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
)

type DataBlob struct {
	rawData []byte
}

func LoadDataBlob(data []byte) (*DataBlob, error) {
	if len(data) < DataBlobHeaderSize {
		return nil, fmt.Errorf("blob too small: %d bytes", len(data))
	}

	blob := &DataBlob{rawData: data}

	magic := blob.Magic()
	if !IsValidBlobMagic(magic) {
		return nil, fmt.Errorf("invalid blob magic: %x", magic)
	}

	if IsEncryptedBlob(magic) && len(data) < EncryptedDataBlobHeaderSize {
		return nil, fmt.Errorf("encrypted blob too small: %d bytes", len(data))
	}

	return blob, nil
}

func LoadDataBlobFromReader(r io.Reader) (*DataBlob, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}

	blob, err := LoadDataBlob(data)
	if err != nil {
		return nil, err
	}

	if err := blob.VerifyCRC(); err != nil {
		return nil, err
	}

	return blob, nil
}

func (b *DataBlob) Magic() *[8]byte {
	var magic [8]byte
	copy(magic[:], b.rawData[0:8])
	return &magic
}

func (b *DataBlob) CRC() uint32 {
	return binary.LittleEndian.Uint32(b.rawData[8:12])
}

func (b *DataBlob) ComputeCRC() uint32 {
	h := crc32.NewIEEE()
	h.Write(b.rawData[HeaderSize(b.Magic()):])
	return h.Sum32()
}

func (b *DataBlob) VerifyCRC() error {
	expected := b.ComputeCRC()
	actual := b.CRC()
	if expected != actual {
		return fmt.Errorf("CRC mismatch: computed %x, stored %x", expected, actual)
	}
	return nil
}

func (b *DataBlob) IsEncrypted() bool {
	return IsEncryptedBlob(b.Magic())
}

func (b *DataBlob) IsCompressed() bool {
	return IsCompressedBlob(b.Magic())
}

func (b *DataBlob) RawData() []byte {
	return b.rawData
}

func (b *DataBlob) Decode(config *CryptConfig, digest *[32]byte) ([]byte, error) {
	magic := b.Magic()

	switch {
	case bytes.Equal(magic[:], UncompressedBlobMagic10[:]):
		return b.decodeUncompressed(digest)

	case bytes.Equal(magic[:], CompressedBlobMagic10[:]):
		return b.decodeCompressed(digest)

	case bytes.Equal(magic[:], EncryptedBlobMagic10[:]):
		return b.decodeEncrypted(config, false, digest)

	case bytes.Equal(magic[:], EncryptedCompressedBlobMagic10[:]):
		return b.decodeEncrypted(config, true, digest)

	default:
		return nil, fmt.Errorf("unknown blob magic: %x", magic)
	}
}

func (b *DataBlob) DecodeRaw(config *CryptConfig, _ *[32]byte) ([]byte, error) {
	magic := b.Magic()
	headerSize := HeaderSize(magic)

	if IsEncryptedBlob(magic) {
		if config == nil {
			return nil, fmt.Errorf("encryption key required")
		}

		var iv [16]byte
		var tag [16]byte
		copy(iv[:], b.rawData[12:28])
		copy(tag[:], b.rawData[28:44])

		encryptedData := b.rawData[EncryptedDataBlobHeaderSize:]
		decrypted, err := config.Decrypt(encryptedData, iv[:], tag[:])
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
		}

		if IsCompressedBlob(magic) {
			decoder, err := zstd.NewReader(bytes.NewReader(decrypted))
			if err != nil {
				return nil, fmt.Errorf("create zstd decoder: %w", err)
			}
			defer decoder.Close()

			data, err := io.ReadAll(decoder)
			if err != nil {
				return nil, fmt.Errorf("decompress: %w", err)
			}
			return data, nil
		}

		return decrypted, nil
	}

	data := b.rawData[headerSize:]
	if IsCompressedBlob(magic) {
		decoder, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", err)
		}
		defer decoder.Close()

		decompressed, err := io.ReadAll(decoder)
		if err != nil {
			return nil, fmt.Errorf("decompress: %w", err)
		}
		return decompressed, nil
	}

	return data, nil
}

func (b *DataBlob) decodeUncompressed(digest *[32]byte) ([]byte, error) {
	data := b.rawData[DataBlobHeaderSize:]
	if digest != nil {
		if err := verifyDigest(data, nil, digest); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (b *DataBlob) decodeCompressed(digest *[32]byte) ([]byte, error) {
	compressed := b.rawData[DataBlobHeaderSize:]
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	defer decoder.Close()

	data, err := io.ReadAll(decoder)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}

	if digest != nil {
		if err := verifyDigest(data, nil, digest); err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (b *DataBlob) decodeEncrypted(config *CryptConfig, compressed bool, digest *[32]byte) ([]byte, error) {
	if config == nil {
		return nil, fmt.Errorf("encryption key required")
	}

	var iv [16]byte
	var tag [16]byte
	copy(iv[:], b.rawData[12:28])
	copy(tag[:], b.rawData[28:44])

	encryptedData := b.rawData[EncryptedDataBlobHeaderSize:]

	decrypted, err := config.Decrypt(encryptedData, iv[:], tag[:])
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	var data []byte
	if compressed {
		decoder, err := zstd.NewReader(bytes.NewReader(decrypted))
		if err != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", err)
		}
		defer decoder.Close()

		data, err = io.ReadAll(decoder)
		if err != nil {
			return nil, fmt.Errorf("decompress: %w", err)
		}
	} else {
		data = decrypted
	}

	if digest != nil {
		if err := verifyDigest(data, config, digest); err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (b *DataBlob) GetEncryptedHeader() (iv *[16]byte, tag *[16]byte, err error) {
	if !b.IsEncrypted() {
		return nil, nil, fmt.Errorf("not an encrypted blob")
	}

	iv = &[16]byte{}
	tag = &[16]byte{}
	copy(iv[:], b.rawData[12:28])
	copy(tag[:], b.rawData[28:44])
	return iv, tag, nil
}

func IsValidBlobMagic(magic *[8]byte) bool {
	return bytes.Equal(magic[:], UncompressedBlobMagic10[:]) ||
		bytes.Equal(magic[:], CompressedBlobMagic10[:]) ||
		bytes.Equal(magic[:], EncryptedBlobMagic10[:]) ||
		bytes.Equal(magic[:], EncryptedCompressedBlobMagic10[:])
}

func verifyDigest(data []byte, config *CryptConfig, expected *[32]byte) error {
	var digest [32]byte
	if config != nil {
		digest = config.ComputeDigest(data)
	} else {
		digest = Sha256Sum(data)
	}

	if !bytes.Equal(digest[:], expected[:]) {
		return fmt.Errorf("digest mismatch")
	}
	return nil
}

const MaxBlobSize = 128 * 1024 * 1024

func EncodeDataBlob(data []byte, config *CryptConfig, compress bool) (*DataBlob, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("data too large: %d bytes", len(data))
	}

	headerLen := DataBlobHeaderSize
	if config != nil {
		headerLen = EncryptedDataBlobHeaderSize
	}

	var compressedData []byte
	didCompress := false

	if compress {
		var buf bytes.Buffer
		encoder, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, fmt.Errorf("create zstd encoder: %w", err)
		}

		if _, err := encoder.Write(data); err != nil {
			encoder.Close()
			return nil, fmt.Errorf("compress: %w", err)
		}
		encoder.Close()

		compressedData = buf.Bytes()
		if len(compressedData) < len(data) {
			didCompress = true
		}
	}

	var magic [8]byte
	var encryptionSource []byte

	switch {
	case config != nil && didCompress:
		magic = EncryptedCompressedBlobMagic10
		encryptionSource = compressedData
	case config != nil:
		magic = EncryptedBlobMagic10
		encryptionSource = data
	case didCompress:
		magic = CompressedBlobMagic10
	default:
		magic = UncompressedBlobMagic10
	}

	var rawData []byte

	if config != nil {
		rawData = make([]byte, headerLen, headerLen+len(encryptionSource)+64)
		copy(rawData[0:8], magic[:])

		iv, tag, encrypted, err := config.Encrypt(encryptionSource)
		if err != nil {
			return nil, fmt.Errorf("encrypt: %w", err)
		}

		copy(rawData[12:28], iv[:])
		copy(rawData[28:44], tag[:])
		rawData = append(rawData, encrypted...)
	} else {
		var content []byte
		if didCompress {
			content = compressedData
		} else {
			content = data
		}
		rawData = make([]byte, headerLen+len(content))
		copy(rawData[0:8], magic[:])
		copy(rawData[headerLen:], content)
	}

	blob := &DataBlob{rawData: rawData}
	crc := blob.ComputeCRC()
	binary.LittleEndian.PutUint32(blob.rawData[8:12], crc)

	return blob, nil
}

func CompressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()
	return buf.Bytes(), nil
}
