package types

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc/arpcdata"
)

// FileHandleId is a type alias for uint64
type FileHandleId uint64

func (id *FileHandleId) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8)
	if err := enc.WriteUint64(uint64(*id)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (id *FileHandleId) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	value, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	*id = FileHandleId(value)
	return nil
}

// ReadDirEntries is a slice of VSSDirEntry
type ReadDirEntries []VSSDirEntry

func (entries *ReadDirEntries) Encode() ([]byte, error) {
	// Create an encoder with an estimated size
	enc := arpcdata.NewEncoder()

	// Write the number of entries as a uint32
	if err := enc.WriteUint32(uint32(len(*entries))); err != nil {
		return nil, err
	}

	// Encode each entry and append it to the encoder
	for _, entry := range *entries {
		entryBytes, err := entry.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(entryBytes); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

// DecodeVSSDirEntries decodes a byte slice into an array of VSSDirEntry
func (entries *ReadDirEntries) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	// Read the number of entries
	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Decode each entry
	*entries = make([]VSSDirEntry, count)
	for i := uint32(0); i < count; i++ {
		entryBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var entry VSSDirEntry
		if err := entry.Decode(entryBytes); err != nil {
			return err
		}
		(*entries)[i] = entry
	}

	return nil
}
