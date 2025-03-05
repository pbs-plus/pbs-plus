package types

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc/arpcdata"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/sb"
)

// FileHandleId is a type alias for uint64
type FileHandleId uint64

func (id *FileHandleId) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()
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
	enc := arpcdata.NewEncoder()

	// Write the number of entries
	if err := enc.WriteUint32(uint32(len(*entries))); err != nil {
		return nil, err
	}

	// Write each VSSDirEntry
	for _, entry := range *entries {
		if err := enc.WriteBytes(sb.ToBytes(entry.Name)); err != nil {
			return nil, err
		}
		if err := enc.WriteUint32(entry.Mode); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

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

	// Read each VSSDirEntry
	*entries = make([]VSSDirEntry, count)
	for i := 0; i < int(count); i++ {
		name, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		mode, err := dec.ReadUint32()
		if err != nil {
			return err
		}
		(*entries)[i] = VSSDirEntry{
			Name: sb.ToString(name),
			Mode: mode,
		}
	}

	return nil
}
