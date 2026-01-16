//go:build linux

package blksnap

import (
	"fmt"
	"log"
	"time"
)

func main() {
	// Open control device
	ctrl, err := OpenControl()
	if err != nil {
		log.Fatalf("Failed to open control: %v", err)
	}
	defer ctrl.Close()

	// Get version
	version, err := ctrl.GetVersion()
	if err != nil {
		log.Fatalf("Failed to get version: %v", err)
	}
	fmt.Printf("Blksnap version: %s\n", version)

	// Create a snapshot with diff storage
	diffStoragePath := "/var/backup/diff.img"
	limitSectors := uint64(20 * 1024 * 1024 * 1024 / 512) // 20GB in sectors

	snapshot, err := ctrl.CreateSnapshot(diffStoragePath, limitSectors)
	if err != nil {
		log.Fatalf("Failed to create snapshot: %v", err)
	}
	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	defer snapshot.Destroy()

	// Open and attach tracker to block device
	tracker, err := NewTracker("/dev/sda1")
	if err != nil {
		log.Fatalf("Failed to create tracker: %v", err)
	}
	defer tracker.Close()
	defer tracker.Detach()

	// Attach the filter
	if err := tracker.Attach(); err != nil {
		log.Fatalf("Failed to attach filter: %v", err)
	}

	// Add device to snapshot
	if err := tracker.AddToSnapshot(snapshot.ID); err != nil {
		log.Fatalf("Failed to add device to snapshot: %v", err)
	}

	// Get CBT info
	cbtInfo, err := tracker.GetCbtInfo()
	if err != nil {
		log.Fatalf("Failed to get CBT info: %v", err)
	}
	fmt.Printf("CBT Generation ID: %s\n", cbtInfo.GenerationID)
	fmt.Printf("Block size: %d, Block count: %d, Changes: %d\n",
		cbtInfo.BlockSize, cbtInfo.BlockCount, cbtInfo.ChangesNumber)

	// Take the snapshot
	if err := snapshot.Take(); err != nil {
		log.Fatalf("Failed to take snapshot: %v", err)
	}
	fmt.Println("Snapshot taken successfully")

	// Check snapshot info
	snapInfo, err := tracker.GetSnapshotInfo()
	if err != nil {
		log.Fatalf("Failed to get snapshot info: %v", err)
	}
	fmt.Printf("Snapshot image: %s, error code: %d\n",
		snapInfo.ImageName(), snapInfo.ErrorCode)

	// Monitor for events
	go func() {
		for {
			event, err := ctrl.WaitEvent(5000) // 5 second timeout
			if err != nil {
				log.Printf("Error waiting for event: %v", err)
				continue
			}

			switch event.Code {
			case BlksnapEventCodeCorrupted:
				corrupted := event.GetCorrupted()
				log.Printf("Corrupted event: dev %d:%d, error %d\n",
					corrupted.DevIDMj, corrupted.DevIDMn, corrupted.ErrCode)
			case BlksnapEventCodeNoSpace:
				noSpace := event.GetNoSpace()
				log.Printf("No space event: requested %d sectors\n",
					noSpace.RequestedNrSect)
			}
		}
	}()

	time.Sleep(30 * time.Second)
}
