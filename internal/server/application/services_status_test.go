//go:build linux

package application

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestTargetServiceCheckStatusByType(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	bytesRead := make(chan int, 4)
	go func() {
		for range 4 {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			buf := make([]byte, 1)
			n, _ := conn.Read(buf)
			_ = conn.Close()
			bytesRead <- n
		}
	}()

	localPath := t.TempDir()
	targets := []coredb.Target{
		{Name: "local", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: localPath},
		{Name: "s3", Type: coredb.TargetTypeS3, S3Info: &coredb.S3Url{Endpoint: listener.Addr().String()}},
		{Name: "postgresql", Type: coredb.TargetTypePostgreSQL, DatabaseHost: host, DatabasePort: port},
		{Name: "mysql", Type: coredb.TargetTypeMySQL, DatabaseHost: host, DatabasePort: port},
		{Name: "ldap", Type: coredb.TargetTypeLDAP, DatabaseHost: host, DatabasePort: port},
		{Name: "missing-local", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: localPath + "/missing"},
		{Name: "disconnected-agent", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessAgent, AgentHost: coredb.AgentHost{Name: "offline"}, VolumeTotalBytes: 100, VolumeUsedBytes: 60, VolumeFreeBytes: 40},
	}

	service := NewTargetService(nil, arpc.NewAgentsManager())
	results := service.CheckStatus(t.Context(), targets, true, time.Second, len(targets))
	for i, want := range []bool{true, true, true, true, true, false, false} {
		if results[i].ConnectionStatus != want {
			t.Errorf("target %q status = %v, want %v (error: %v)", targets[i].Name, results[i].ConnectionStatus, want, results[i].Error)
		}
	}
	if results[0].VolumeTotalBytes <= 0 || results[0].VolumeUsedBytes+results[0].VolumeFreeBytes != results[0].VolumeTotalBytes {
		t.Errorf("local target size = %#v", results[0].TargetSizeResult)
	}
	for _, i := range []int{1, 2, 3, 4} {
		if results[i].VolumeTotalBytes != 0 || results[i].VolumeUsedBytes != 0 || results[i].VolumeFreeBytes != 0 {
			t.Errorf("remote target %q resolved content size: %#v", targets[i].Name, results[i].TargetSizeResult)
		}
	}
	if results[6].TargetSizeResult != (TargetSizeResult{VolumeTotalBytes: 100, VolumeUsedBytes: 60, VolumeFreeBytes: 40}) {
		t.Errorf("agent target size = %#v", results[5].TargetSizeResult)
	}

	for range 3 {
		select {
		case n := <-bytesRead:
			if n != 0 {
				t.Errorf("network status probe sent %d bytes", n)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("network status probe did not connect")
		}
	}
}
