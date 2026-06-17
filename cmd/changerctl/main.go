// cmd/changerctl: inspect a SCSI Medium Changer (pure Go, no mtx).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/changer"
)

func main() {
	dev := flag.String("dev", "", "changer device (e.g. /dev/sg1)")
	scan := flag.Bool("inventory", false, "force barcode re-scan before status")
	flag.Parse()
	if *dev == "" {
		fmt.Fprintln(os.Stderr, "usage: changerctl -dev /dev/sg1 [-inventory]")
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
