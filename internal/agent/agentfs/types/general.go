package types

import (
	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
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
	defer arpcdata.ReleaseDecoder(dec)

	value, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	*id = FileHandleId(value)
	return nil
}

// ReadDirEntries is a slice of AgentDirEntry
type ReadDirEntries []AgentDirEntry

func (entries *ReadDirEntries) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteUint32(uint32(len(*entries))); err != nil {
		return nil, err
	}

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

// Decode uses zero-copy techniques to minimize allocations
func (entries *ReadDirEntries) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Reuse existing slice capacity if possible
	if cap(*entries) >= int(count) {
		*entries = (*entries)[:count]
		// Reset existing entries to avoid stale data
		for i := range *entries {
			(*entries)[i].Reset()
		}
	} else {
		*entries = make([]AgentDirEntry, count)
	}

	// Decode entries directly without intermediate allocations
	for i := uint32(0); i < count; i++ {
		entryBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}

		if err := (*entries)[i].Decode(entryBytes); err != nil {
			return err
		}
	}

	return nil
}

