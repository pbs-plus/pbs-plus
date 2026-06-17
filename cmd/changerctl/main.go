// cmd/changerctl: inspect and drive a SCSI Medium Changer (pure Go, no mtx).
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/changer"
)

func main() {
	dev := flag.String("dev", "", "changer device (e.g. /dev/sg1)")
	scan := flag.Bool("inventory", false, "force barcode re-scan before status")
	load := flag.Int("load", 0, "move cartridge from this 1-based storage slot into the drive")
	unload := flag.Bool("unload", false, "move cartridge from the drive back to its source slot")
	drive := flag.Int("drive", 0, "0-based drive index")
	to := flag.Int("to", 0, "with -unload: destination slot (1-based); 0 = original source slot")
	flag.Parse()

	if *dev == "" {
		fmt.Fprintln(os.Stderr, "usage: changerctl -dev /dev/sg1 [-inventory] [-load N] [-unload [-to M]] [-drive D]")
		os.Exit(1)
	}

	c, err := changer.Open(*dev)
	if err != nil {
		die(err)
	}
	defer func() { _ = c.Close() }()

	if *scan {
		fmt.Fprintln(os.Stderr, "running inventory (may take a minute)...")
		if err := c.Inventory(); err != nil {
			fmt.Fprintf(os.Stderr, "inventory warning: %v\n", err)
		}
	}

	st, err := c.Status()
	if err != nil {
		die(err)
	}

	switch {
	case *load > 0:
		loadCmd(c, st, *load, *drive)
	case *unload:
		unloadCmd(c, st, *drive, *to)
	default:
		printStatus(st)
	}
}

func loadCmd(c *changer.Changer, st *changer.Status, slot, drive int) {
	src, _ := st.SlotAddress(slot)
	fmt.Fprintf(os.Stderr, "loading slot %d (addr %d) -> drive %d ...\n", slot, src, drive)
	if err := c.Load(st, slot, drive); err != nil {
		die(err)
	}
	fmt.Fprintf(os.Stderr, "load complete; verifying ...\n")
	if err := verifyDrive(c, drive, true, ""); err != nil {
		die(err)
	}
	fmt.Fprintln(os.Stderr, "ok")
}

func unloadCmd(c *changer.Changer, st *changer.Status, drive, to int) {
	// Remember the loaded cartridge before we move it, so we can confirm.
	src, _ := st.DriveAddress(drive)
	wantTag := ""
	if drive >= 0 && drive < len(st.Drives) {
		wantTag = st.Drives[drive].VolumeTag
	}

	dstSlot := to
	if dstSlot == 0 {
		// DriveStatus.LoadedSlotAddr is the SCSI element address the cartridge
		// came from; map it back to a 1-based slot number.
		loaded := uint16(0)
		if drive >= 0 && drive < len(st.Drives) {
			loaded = st.Drives[drive].LoadedSlotAddr
		}
		for i, s := range st.Slots {
			if s.ElementAddress == loaded && loaded != 0 {
				dstSlot = i + 1
				break
			}
		}
		if dstSlot == 0 {
			fmt.Fprintln(os.Stderr, "warning: could not determine source slot; specify -to")
			os.Exit(1)
		}
	}
	dst, _ := st.SlotAddress(dstSlot)
	fmt.Fprintf(os.Stderr, "unloading drive %d (addr %d) -> slot %d (addr %d) ...\n", drive, src, dstSlot, dst)
	if err := c.Unload(st, drive, dstSlot); err != nil {
		die(err)
	}
	fmt.Fprintf(os.Stderr, "unload complete; verifying ...\n")
	if err := verifyDrive(c, drive, false, wantTag); err != nil {
		die(err)
	}
	fmt.Fprintln(os.Stderr, "ok")
}

// verifyDrive re-reads element status to confirm the drive is in the expected
// full/empty state after a move.
func verifyDrive(c *changer.Changer, drive int, wantFull bool, wantTag string) error {
	st, err := c.Status()
	if err != nil {
		return err
	}
	if drive < 0 || drive >= len(st.Drives) {
		return fmt.Errorf("drive %d out of range", drive)
	}
	d := st.Drives[drive]
	if d.Full != wantFull {
		return fmt.Errorf("drive %d full=%v, want %v", drive, d.Full, wantFull)
	}
	if wantTag != "" && d.VolumeTag != "" && d.VolumeTag != wantTag {
		return fmt.Errorf("drive %d voltag=%q, want %q", drive, d.VolumeTag, wantTag)
	}
	_ = strconv.Itoa // keep strconv import used if wantTag path grows
	return nil
}

func printStatus(st *changer.Status) {
	fmt.Printf("transport (picker): element 0x%04x\n", st.TransportAddress)
	fmt.Printf("\n%d storage slot(s):\n", len(st.Slots))
	for i, s := range st.Slots {
		tag := s.VolumeTag
		if tag == "" {
			tag = "-"
		}
		kind := "storage"
		if s.ImportExport {
			kind = "import/export"
		}
		fmt.Printf("  slot %2d  addr=%-5d %-13s %v  voltag=%s\n", i+1, s.ElementAddress, kind, state(s.Full), tag)
	}
	fmt.Printf("\n%d drive(s):\n", len(st.Drives))
	for i, d := range st.Drives {
		tag := d.VolumeTag
		if tag == "" {
			tag = "-"
		}
		fmt.Printf("  drive %d  addr=%-5d %v  voltag=%s\n", i, d.ElementAddress, state(d.Full), tag)
	}
}

func state(full bool) string {
	if full {
		return "FULL "
	}
	return "empty"
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
