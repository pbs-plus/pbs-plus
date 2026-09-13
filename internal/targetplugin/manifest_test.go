package targetplugin

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestParsePluginManifest(t *testing.T) {
	manifest, _ := testPluginManifest(t)
	data, err := toml.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	digest := sha256.Sum256(data)

	parsed, err := ParsePluginManifest(data, hex.EncodeToString(digest[:]))
	if err != nil {
		t.Fatalf("ParsePluginManifest: %v", err)
	}
	if !reflect.DeepEqual(parsed, manifest) {
		t.Fatalf("manifest = %#v, want %#v", parsed, manifest)
	}
}

func TestParsePluginManifestRejectsInvalidInput(t *testing.T) {
	manifest, _ := testPluginManifest(t)
	badSchema := manifest
	badSchema.SchemaSHA256 = strings.Repeat("00", sha256.Size)
	badSchemaData, err := toml.Marshal(badSchema)
	if err != nil {
		t.Fatalf("marshal bad schema manifest: %v", err)
	}
	badSchemaDigest := sha256.Sum256(badSchemaData)

	validData, err := toml.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	unknownData := append([]byte("unknown = true\n"), validData...)
	unknownDigest := sha256.Sum256(unknownData)

	tests := []struct {
		name          string
		data          []byte
		expected      string
		wantError     string
		wantDigestErr bool
	}{
		{
			name:          "authenticate before decode",
			data:          []byte("not valid TOML = ["),
			expected:      strings.Repeat("00", sha256.Size),
			wantError:     ErrInvalidManifestDigest.Error(),
			wantDigestErr: true,
		},
		{
			name:      "unknown field",
			data:      unknownData,
			expected:  hex.EncodeToString(unknownDigest[:]),
			wantError: "unknown field",
		},
		{
			name:      "schema digest",
			data:      badSchemaData,
			expected:  hex.EncodeToString(badSchemaDigest[:]),
			wantError: "does not match manifest forms",
		},
		{
			name:      "oversized",
			data:      make([]byte, MaxManifestBytes+1),
			expected:  strings.Repeat("00", sha256.Size),
			wantError: "exceeds",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParsePluginManifest(test.data, test.expected)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("ParsePluginManifest error = %v, want %q", err, test.wantError)
			}
			if test.wantDigestErr && !errors.Is(err, ErrInvalidManifestDigest) {
				t.Fatalf("ParsePluginManifest error = %v, want ErrInvalidManifestDigest", err)
			}
		})
	}
}

func TestPluginManifestVerifyRelease(t *testing.T) {
	manifest, _ := testPluginManifest(t)
	release := validRepositoryIndex(t).Releases[0]
	if err := manifest.VerifyRelease(release); err != nil {
		t.Fatalf("VerifyRelease: %v", err)
	}

	release.PluginID = "org.pbs-plus.other"
	if err := manifest.VerifyRelease(release); err == nil || !strings.Contains(err.Error(), "plugin ID does not match") {
		t.Fatalf("VerifyRelease error = %v", err)
	}
}

func TestPluginManifestVerifyDescriptor(t *testing.T) {
	manifest, descriptor := testPluginManifest(t)
	if err := manifest.VerifyDescriptor(descriptor); err != nil {
		t.Fatalf("VerifyDescriptor: %v", err)
	}

	wrongIdentity := descriptor
	wrongIdentity.PluginID = "org.pbs-plus.other"
	if err := manifest.VerifyDescriptor(wrongIdentity); err == nil || !strings.Contains(err.Error(), "plugin ID does not match") {
		t.Fatalf("VerifyDescriptor identity error = %v", err)
	}

	wrongSchema := descriptor
	wrongSchema.RestoreSchema.Version++
	if err := manifest.VerifyDescriptor(wrongSchema); err == nil || !strings.Contains(err.Error(), "schemas do not match") {
		t.Fatalf("VerifyDescriptor schema error = %v", err)
	}
}

func TestDescriptorSchemaDigestFixture(t *testing.T) {
	_, descriptor := testPluginManifest(t)
	digest, err := descriptorSchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("descriptorSchemaDigest: %v", err)
	}
	const want = "83c7736ded9adc903cd51f0e515a69d9cf10b6e0e89b5caaa38eaf27818b4089"
	if digest != want {
		t.Fatalf("descriptor schema digest = %s, want %s", digest, want)
	}
}

func testPluginManifest(t *testing.T) (PluginManifest, Descriptor) {
	t.Helper()
	descriptor := Descriptor{
		ProtocolVersion: CurrentProtocolVersion,
		PluginID:        "org.pbs-plus.filesystem",
		Version:         "1.0.0",
		TargetTypes:     []string{"filesystem"},
		TargetSchema: FormSchema{
			Version: 1,
			Fields: []FormField{
				{Key: "path", Label: "Path", Control: ControlPath, Required: true},
			},
		},
		BackupSchema:  FormSchema{Version: 1},
		RestoreSchema: FormSchema{Version: 1},
	}
	digest, err := descriptorSchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("descriptorSchemaDigest: %v", err)
	}
	return PluginManifest{
		FormatVersion:   ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    digest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	}, descriptor
}
