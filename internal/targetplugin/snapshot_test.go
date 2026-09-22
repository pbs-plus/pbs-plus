package targetplugin

import (
	"bytes"
	"testing"
)

func TestSnapshotMetadataCanonicalRoundTrip(t *testing.T) {
	metadata := SnapshotMetadata{
		FormatVersion:       SnapshotMetadataFormatVersion,
		PluginID:            "org.pbs-plus.test",
		PluginVersion:       "1.2.3",
		TargetType:          "test",
		TargetSchemaVersion: 2,
		BackupSchemaVersion: 3,
		Archive:             Archive{Type: "test", FormatVersion: 4},
	}
	encoded, err := MarshalProtocol(metadata)
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	encodedAgain, err := MarshalProtocol(metadata)
	if err != nil {
		t.Fatalf("MarshalProtocol again: %v", err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("snapshot metadata encoding is not deterministic")
	}
	var decoded SnapshotMetadata
	if err := UnmarshalProtocol(encoded, &decoded); err != nil {
		t.Fatalf("UnmarshalProtocol: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if decoded != metadata {
		t.Fatalf("decoded metadata = %#v", decoded)
	}
}

func TestSnapshotMetadataRejectsIncompleteIdentity(t *testing.T) {
	metadata := SnapshotMetadata{
		FormatVersion:       SnapshotMetadataFormatVersion,
		PluginID:            "org.pbs-plus.test",
		PluginVersion:       "1.2.3",
		TargetType:          "test",
		TargetSchemaVersion: 2,
		Archive:             Archive{Type: "test", FormatVersion: 4},
	}
	if err := metadata.Validate(); err == nil {
		t.Fatal("metadata without backup schema version accepted")
	}
}
