//go:build linux

package pxarmount

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxarfuse "github.com/pbs-plus/pbs-plus/internal/backend/vfs/pxar/fuse"
	native "github.com/pbs-plus/pbs-plus/internal/pxar/native"
)

type PxarMount struct {
	Mountpoint   string
	MetaPath     string
	PayloadPath  string
	PbsStoreRoot string

	metaIndex    *native.DynamicIndexReader
	payloadIndex *native.DynamicIndexReader
	server       *fuse.Server
}

func Mount(pbsStoreRoot, mpxarPath, ppxarPath, mountpoint string) (*PxarMount, error) {
	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	metaIndex, err := native.OpenDynamicIndex(mpxarPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata index: %w", err)
	}

	var payloadIndex *native.DynamicIndexReader
	if ppxarPath != "" {
		payloadIndex, err = native.OpenDynamicIndex(ppxarPath)
		if err != nil {
			metaIndex.Close()
			return nil, fmt.Errorf("open payload index: %w", err)
		}
	} else {
		payloadIndex = metaIndex
	}

	var cryptConfig *native.CryptConfig
	chunkStore := native.NewChunkStore(pbsStoreRoot, cryptConfig, false)

	metaReader := native.NewBufferedReader(metaIndex, chunkStore)
	var payloadReader *native.BufferedReader
	if ppxarPath != "" {
		payloadReader = native.NewBufferedReader(payloadIndex, chunkStore)
	} else {
		payloadReader = metaReader
	}

	accessor, err := native.NewAccessor(metaReader, metaIndex.ArchiveSize(), payloadReader, payloadIndex.ArchiveSize())
	if err != nil {
		metaIndex.Close()
		if ppxarPath != "" {
			payloadIndex.Close()
		}
		return nil, fmt.Errorf("create accessor: %w", err)
	}

	fsName := "pbs-plus-pxar://" + mpxarPath

	server, err := pxarfuse.Mount(mountpoint, fsName, accessor)
	if err != nil {
		metaIndex.Close()
		if ppxarPath != "" {
			payloadIndex.Close()
		}
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	pm := &PxarMount{
		Mountpoint:   mountpoint,
		MetaPath:     mpxarPath,
		PayloadPath:  ppxarPath,
		PbsStoreRoot: pbsStoreRoot,
		metaIndex:    metaIndex,
		payloadIndex: payloadIndex,
		server:       server,
	}

	return pm, nil
}

func (pm *PxarMount) Wait() {
	if pm.server != nil {
		_ = pm.server.WaitMount()
	}
}

func (pm *PxarMount) Close() error {
	if pm.metaIndex != nil {
		pm.metaIndex.Close()
	}
	if pm.payloadIndex != nil && pm.payloadIndex != pm.metaIndex {
		pm.payloadIndex.Close()
	}
	return nil
}

func RunMount(pbsStoreRoot, mpxarPath, ppxarPath, mountpoint string) error {
	pm, err := Mount(pbsStoreRoot, mpxarPath, ppxarPath, mountpoint)
	if err != nil {
		return err
	}
	defer pm.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	umount := exec.Command("umount", "-lf", mountpoint)
	_ = umount.Run()

	return nil
}
