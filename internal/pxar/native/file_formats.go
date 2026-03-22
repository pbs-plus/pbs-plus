package native

var (
	ProxmoxCatalogFileMagic10      = [8]byte{145, 253, 96, 249, 196, 103, 88, 213}
	UncompressedBlobMagic10        = [8]byte{66, 171, 56, 7, 190, 131, 112, 161}
	CompressedBlobMagic10          = [8]byte{49, 185, 88, 66, 111, 182, 163, 127}
	EncryptedBlobMagic10           = [8]byte{123, 103, 133, 190, 34, 45, 76, 240}
	EncryptedCompressedBlobMagic10 = [8]byte{230, 89, 27, 191, 11, 191, 216, 11}
	FixedSizedChunkIndex10         = [8]byte{47, 127, 65, 237, 145, 253, 15, 205}
	DynamicSizedChunkIndex10       = [8]byte{28, 145, 78, 165, 25, 186, 179, 205}
)

const (
	DataBlobHeaderSize          = 12
	EncryptedDataBlobHeaderSize = 44
	DynamicIndexHeaderSize      = 4096
	DynamicIndexEntrySize       = 40
)

type DataBlobHeader struct {
	Magic [8]byte
	CRC   [4]byte
}

type EncryptedDataBlobHeader struct {
	Head DataBlobHeader
	IV   [16]byte
	Tag  [16]byte
}

func IsEncryptedBlob(magic *[8]byte) bool {
	return *magic == EncryptedBlobMagic10 || *magic == EncryptedCompressedBlobMagic10
}

func IsCompressedBlob(magic *[8]byte) bool {
	return *magic == CompressedBlobMagic10 || *magic == EncryptedCompressedBlobMagic10
}

func HeaderSize(magic *[8]byte) int {
	if IsEncryptedBlob(magic) {
		return EncryptedDataBlobHeaderSize
	}
	return DataBlobHeaderSize
}
