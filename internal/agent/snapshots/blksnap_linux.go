//go:build linux

package snapshots

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots/blksnap"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots/veeamsnap"
)

type SnapshotBackend interface {
	CreateSnapshot(jobId string, sourcePath string, diffPath string, limitSectors uint64) (snapID string, snapDevice string, err error)
	DestroySnapshot(snapID string) error
	IsAvailable() bool
}

func generateUUIDFromJobID(jobID string) [16]byte {
	hash := sha256.Sum256([]byte(jobID))
	var uuid [16]byte
	copy(uuid[:], hash[:16])

	uuid[6] = (uuid[6] & 0x0F) | 0x50
	uuid[8] = (uuid[8] & 0x3F) | 0x80

	return uuid
}

func generateSnapshotIDFromJobID(jobID string) uint64 {
	hash := sha256.Sum256([]byte(jobID))
	var id uint64
	for i := 0; i < 8; i++ {
		id = (id << 8) | uint64(hash[i])
	}
	if id == 0 {
		id = 1
	}
	return id
}

type BlksnapBackend struct{}

func (b *BlksnapBackend) CreateSnapshot(jobId string, sourcePath string, diffPath string, limitSectors uint64) (string, string, error) {
	devicePath, err := getDeviceForPath(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to find device for %s: %w", sourcePath, err)
	}

	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return "", "", fmt.Errorf("failed to open blksnap control: %w", err)
	}
	defer ctrl.Close()

	snap, err := ctrl.CreateSnapshot(diffPath, limitSectors)
	if err != nil {
		return "", "", fmt.Errorf("failed to create blksnap session: %w", err)
	}

	tracker, err := blksnap.NewTracker(devicePath)
	if err != nil {
		snap.Destroy()
		return "", "", fmt.Errorf("failed to create tracker for %s: %w", devicePath, err)
	}
	defer tracker.Close()

	if err := tracker.Attach(); err != nil {
		snap.Destroy()
		return "", "", fmt.Errorf("failed to attach blksnap filter: %w", err)
	}

	if err := tracker.AddToSnapshot(snap.ID); err != nil {
		snap.Destroy()
		return "", "", fmt.Errorf("failed to add device to snapshot group: %w", err)
	}

	if err := snap.Take(); err != nil {
		snap.Destroy()
		return "", "", fmt.Errorf("failed to take blksnap: %w", err)
	}

	snapInfo, err := tracker.GetSnapshotInfo()
	if err != nil {
		snap.Destroy()
		return "", "", fmt.Errorf("failed to get snapshot info: %w", err)
	}

	return snap.ID.String(), snapInfo.ImageName(), nil
}

func (b *BlksnapBackend) DestroySnapshot(snapID string) error {
	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return fmt.Errorf("failed to open blksnap control: %w", err)
	}
	defer ctrl.Close()

	uuid, err := blksnap.ParseUUID(snapID)
	if err != nil {
		return fmt.Errorf("invalid UUID: %w", err)
	}

	snap := ctrl.OpenSnapshot(uuid)
	return snap.Destroy()
}

func (b *BlksnapBackend) IsAvailable() bool {
	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return false
	}
	ctrl.Close()
	return true
}

type VeeamsnapBackend struct{}

func (v *VeeamsnapBackend) CreateSnapshot(jobId string, sourcePath string, diffPath string, limitSectors uint64) (string, string, error) {
	devicePath, err := getDeviceForPath(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to find device for %s: %w", sourcePath, err)
	}

	devID, err := getDeviceID(devicePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get device ID for %s: %w", devicePath, err)
	}

	ctrl, err := veeamsnap.OpenControl()
	if err != nil {
		return "", "", fmt.Errorf("failed to open veeamsnap control: %w", err)
	}
	defer ctrl.Close()

	snapshotID := generateSnapshotIDFromJobID(jobId)

	// Create tracker with CBT block size (default to 64KB = 128 sectors)
	tracker := veeamsnap.NewTracker(ctrl)
	if err := tracker.AddTracking(devID.Major, devID.Minor); err != nil {
		return "", "", fmt.Errorf("failed to add tracking: %w", err)
	}

	snap, err := ctrl.CreateSnapshot(snapshotID, []veeamsnap.DevID{devID})
	if err != nil {
		tracker.RemoveTracking(devID.Major, devID.Minor)
		return "", "", fmt.Errorf("failed to create snapshot: %w", err)
	}

	snapstoreUUID := veeamsnap.UUID(generateUUIDFromJobID(jobId))

	snapstore, err := ctrl.CreateSnapstore(snapstoreUUID, veeamsnap.DevID{Major: 0, Minor: 0}, []veeamsnap.DevID{devID})
	if err != nil {
		snap.Destroy()
		tracker.RemoveTracking(devID.Major, devID.Minor)
		return "", "", fmt.Errorf("failed to create snapstore: %w", err)
	}

	if err := snapstore.SetMemoryLimit(limitSectors * 512); err != nil {
		snap.Destroy()
		tracker.RemoveTracking(devID.Major, devID.Minor)
		return "", "", fmt.Errorf("failed to set memory limit: %w", err)
	}

	time.Sleep(200 * time.Millisecond)

	images, err := ctrl.CollectSnapshotImages(10)
	if err != nil || len(images) == 0 {
		snap.Destroy()
		tracker.RemoveTracking(devID.Major, devID.Minor)
		return "", "", fmt.Errorf("failed to get snapshot image: %w", err)
	}

	var snapDevice string
	for _, img := range images {
		if img.OriginalDevID.Major == devID.Major && img.OriginalDevID.Minor == devID.Minor {
			snapDevice = fmt.Sprintf("/dev/%s%d", veeamsnap.VeeamSnapImageName, img.SnapshotDevID.Minor)
			break
		}
	}

	if snapDevice == "" {
		snap.Destroy()
		tracker.RemoveTracking(devID.Major, devID.Minor)
		return "", "", fmt.Errorf("snapshot image device not found")
	}

	return fmt.Sprintf("%d", snapshotID), snapDevice, nil
}

func (v *VeeamsnapBackend) DestroySnapshot(snapID string) error {
	ctrl, err := veeamsnap.OpenControl()
	if err != nil {
		return fmt.Errorf("failed to open veeamsnap control: %w", err)
	}
	defer ctrl.Close()

	var snapshotID uint64
	_, err = fmt.Sscanf(snapID, "%d", &snapshotID)
	if err != nil {
		return fmt.Errorf("invalid snapshot ID: %w", err)
	}

	snap := ctrl.OpenSnapshot(snapshotID)
	return snap.Destroy()
}

func (v *VeeamsnapBackend) IsAvailable() bool {
	ctrl, err := veeamsnap.OpenControl()
	if err != nil {
		return false
	}
	ctrl.Close()
	return true
}

type KernelSnapshotHandler struct {
	DiffStorageDir string
	backend        SnapshotBackend
}

func NewKernelSnapshotHandler() *KernelSnapshotHandler {
	handler := &KernelSnapshotHandler{
		DiffStorageDir: "/var/lib/pbs-plus-agent/snapshots",
	}

	blksnapBackend := &BlksnapBackend{}
	if blksnapBackend.IsAvailable() {
		handler.backend = blksnapBackend
		return handler
	}

	veeamsnapBackend := &VeeamsnapBackend{}
	if veeamsnapBackend.IsAvailable() {
		handler.backend = veeamsnapBackend
		return handler
	}

	handler.backend = nil
	return handler
}

func (h *KernelSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	if h.backend == nil {
		return Snapshot{}, fmt.Errorf("no snapshot backend available (neither blksnap nor veeamsnap)")
	}

	if h.DiffStorageDir == "" {
		h.DiffStorageDir = "/var/lib/pbs-plus-agent/snapshots"
	}
	_ = os.MkdirAll(h.DiffStorageDir, 0700)
	diffPath := filepath.Join(h.DiffStorageDir, jobId+".diff")

	freeSectors, err := getFreeSectors(h.DiffStorageDir)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to check free space: %w", err)
	}

	limitSectors := uint64(float64(freeSectors) * 0.8)
	if limitSectors < 2097152 && freeSectors > 2097152 {
		limitSectors = 2097152
	}

	snapID, snapDevice, err := h.backend.CreateSnapshot(jobId, sourcePath, diffPath, limitSectors)
	if err != nil {
		return Snapshot{}, err
	}

	mountPath := filepath.Join(os.TempDir(), "pbs-plus-snapshot", jobId)
	if err := os.MkdirAll(mountPath, 0750); err != nil {
		h.backend.DestroySnapshot(snapID)
		return Snapshot{}, fmt.Errorf("failed to create mount point: %w", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := os.Stat(snapDevice); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mountCmd := exec.Command("mount", "-o", "ro,nouuid", snapDevice, mountPath)
	if output, err := mountCmd.CombinedOutput(); err != nil {
		h.backend.DestroySnapshot(snapID)
		os.Remove(mountPath)
		return Snapshot{}, fmt.Errorf("mount failed: %s, %w", string(output), err)
	}

	return Snapshot{
		Id:          snapID,
		SnapBlock:   snapDevice,
		Path:        mountPath,
		SourcePath:  sourcePath,
		TimeStarted: time.Now(),
		JobId:       jobId,
		Handler:     h,
	}, nil
}

func (h *KernelSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if snapshot.Path != "" {
		_ = exec.Command("umount", "-l", snapshot.Path).Run()
		_ = os.Remove(snapshot.Path)
	}

	if h.backend != nil && snapshot.Id != "" {
		_ = h.backend.DestroySnapshot(snapshot.Id)
	}

	diffPath := filepath.Join(h.DiffStorageDir, snapshot.JobId+".diff")
	_ = os.Remove(diffPath)

	return nil
}

func (h *KernelSnapshotHandler) IsSupported(sourcePath string) bool {
	if h.backend == nil {
		return false
	}

	dev, err := getDeviceForPath(sourcePath)
	if err != nil {
		return false
	}

	return strings.HasPrefix(dev, "/dev/")
}

func getDeviceForPath(path string) (string, error) {
	cmd := exec.Command("df", "--output=source", path)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("could not find device for path")
	}
	return strings.TrimSpace(lines[1]), nil
}

func getDeviceID(devicePath string) (veeamsnap.DevID, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(devicePath, &stat); err != nil {
		return veeamsnap.DevID{}, err
	}

	major := int32(stat.Rdev / 256)
	minor := int32(stat.Rdev % 256)

	return veeamsnap.DevID{
		Major: major,
		Minor: minor,
	}, nil
}

func getFreeSectors(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(dir, &stat)
	if err != nil {
		return 0, err
	}

	availableBytes := stat.Bavail * uint64(stat.Bsize)
	return availableBytes / 512, nil
}
