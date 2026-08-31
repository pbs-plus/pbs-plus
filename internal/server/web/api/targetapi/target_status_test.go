//go:build linux

package targetapi

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestD2DTargetStatusHandlerResolvesRequestedKind(t *testing.T) {
	dir := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(dir, "seal.key"))
	db, err := coredb.Initialize(t.Context(), filepath.Join(dir, "targets.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	service := application.NewTargetService(db, arpc.NewAgentsManager())
	if err := service.CreateTarget(nil, coredb.Target{
		Name: "local",
		Type: coredb.TargetTypeFilesystem,
		Path: t.TempDir(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.CreateTarget(nil, coredb.Target{
		Name: "s3",
		Type: coredb.TargetTypeS3,
		Path: "http://127.0.0.1:1/archive?path-style=true",
	}); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/?kind=filesystem", nil)
	D2DTargetStatusHandler(&application.Runtime{Target: service}).ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("status code = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var statuses map[string]struct {
		ConnectionStatus bool
		VolumeTotalBytes int
		VolumeUsedBytes  int
		VolumeFreeBytes  int
	}
	if err := json.NewDecoder(recorder.Body).Decode(&statuses); err != nil {
		t.Fatal(err)
	}
	local := statuses["local"]
	if len(statuses) != 1 || !local.ConnectionStatus {
		t.Fatalf("statuses = %#v", statuses)
	}
	if local.VolumeTotalBytes <= 0 || local.VolumeUsedBytes+local.VolumeFreeBytes != local.VolumeTotalBytes {
		t.Fatalf("local target size = %#v", local)
	}
}
