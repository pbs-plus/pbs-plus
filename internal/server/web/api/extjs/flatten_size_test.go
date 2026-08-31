package extjs

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestFlattenTargetSizeUsesAvailableMetadata(t *testing.T) {
	unknown := FlattenBackup(coredb.Backup{Target: coredb.Target{Name: "s3", Type: coredb.TargetTypeS3}})
	if unknown.TargetSizeHuman != "" {
		t.Fatalf("unknown target size = %q", unknown.TargetSizeHuman)
	}

	known := FlattenBackup(coredb.Backup{Target: coredb.Target{
		Name:             "agent",
		Type:             coredb.TargetTypeFilesystem,
		Access:           coredb.FilesystemAccessAgent,
		VolumeTotalBytes: 100,
		VolumeUsedBytes:  60,
	}})
	if known.ExpectedSize != 60 || known.TargetSizeHuman != "60 B" {
		t.Fatalf("known target size = %d, %q", known.ExpectedSize, known.TargetSizeHuman)
	}

	empty := FlattenRestore(coredb.Restore{DestTarget: coredb.Target{
		Name:             "empty-local",
		Type:             coredb.TargetTypeFilesystem,
		Access:           coredb.FilesystemAccessLocal,
		VolumeTotalBytes: 100,
	}})
	if empty.TargetSizeHuman != "0 B" {
		t.Fatalf("empty target size = %q", empty.TargetSizeHuman)
	}
}
