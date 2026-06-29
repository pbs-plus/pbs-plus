package main

import (
	"flag"
	"fmt"
	"os"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pbs-plus/internal/tapeio"
)

func main() {
	dev := flag.String("dev", "", "tape device (required)")
	pba := flag.Int64("pba", -1, "seek to this PBA before walking (-1 = sequential from BOT)")
	probeSSET := flag.Int64("probe-sset", -1, "probe candidate PBAs near this stored SSETPBA to find the actual SSET position")
	walkSSET := flag.Bool("walk", false, "walk tape sequentially and report each SSET's actual PBA vs stored SSETPBA")
	verbose := flag.Bool("v", false, "dump every stream header")
	flag.Parse()

	if *dev == "" {
		fmt.Fprintln(os.Stderr, "usage: mtfprobe -dev <tape-device> [-pba N] [-v]")
		os.Exit(2)
	}

	rc, err := tapeio.OpenTapeReader(*dev)
	if err != nil {
		die(err)
	}
	defer rc.Close()

	r := mtf.NewReader(rc)

	if *walkSSET {
		walkSSETMain(rc, r)
		return
	}

	if *probeSSET >= 0 {
		probeSSETMain(rc, r, *probeSSET)
		return
	}

	if *pba >= 0 {
		blk, err := r.Next()
		if err != nil {
			die(fmt.Errorf("read TAPE descriptor: %w", err))
		}
		fmt.Printf("TAPE: name=%q flbsize=%d\n", blk.Tape.Name, blk.Tape.FLBSize)
		fmt.Printf("Seeking to PBA %d\n", *pba)
		if err := r.SeekToBlock(*pba); err != nil {
			die(fmt.Errorf("seek: %w", err))
		}
	}

	nBlk := 0
	nFile, nDir := 0, 0
	for {
		b, err := r.Next()
		if err != nil {
			fmt.Printf("\n(end: %v after %d blocks; files=%d dirs=%d)\n", err, nBlk, nFile, nDir)
			return
		}
		nBlk++
		switch b.Kind {
		case mtf.KindMedia:
			fmt.Printf("%4d MEDIA  flb=%d\n", nBlk, b.Tape.FLBSize)
		case mtf.KindSet:
			fmt.Printf("%4d SET    num=%d name=%q\n", nBlk, b.Set.Number, b.Set.Name)
		case mtf.KindSetEnd:
			fmt.Printf("%4d SETEND (files=%d dirs=%d)\n", nBlk, nFile, nDir)
			return
		case mtf.KindEntry:
			h := b.Header
			switch h.Type {
			case mtf.EntryDirectory:
				nDir++
				fmt.Printf("%4d DIR#%d %s\n", nBlk, nDir, h.Name)
			case mtf.EntryVolume:
				fmt.Printf("%4d VOLB   %s  machine=%s\n", nBlk, h.Name, h.MachineName)
			case mtf.EntryFile:
				nFile++
				fmt.Printf("%4d FILE#%d %s  size=%d\n", nBlk, nFile, h.Name, h.Size)
			default:
				fmt.Printf("%4d ENTRY  type=%d %s\n", nBlk, h.Type, h.Name)
			}
			if *verbose {
				for i, s := range h.Streams {
					fmt.Printf("       stream[%d] id=%q datalen=%d\n", i, streamIDStr(s.Type), len(s.Data))
				}
			}
		}
	}
}

func probeSSETMain(rc *tapeio.TapeReader, _ *mtf.Reader, storedPBA int64) {
	fmt.Println("Rewinding...")
	if err := rc.Rewind(); err != nil {
		die(err)
	}

	r := mtf.NewReader(rc)
	blk, err := r.Next()
	if err != nil {
		die(fmt.Errorf("read TAPE: %w", err))
	}
	fmt.Printf("TAPE: name=%q\n", blk.Tape.Name)

	pos, err := rc.TellBlock()
	if err != nil {
		fmt.Printf("TellBlock: %v\n", err)
	}
	fmt.Printf("TellBlock after TAPE: %d\n", pos)

	sm, err := mtf.ReadSetMap(rc)
	if err != nil {
		die(fmt.Errorf("ReadSetMap: %w", err))
	}
	firstPBA := int64(sm.Entries[0].SSETPBA)
	fmt.Printf("SetMap: %d entries, first SSETPBA=%d\n", len(sm.Entries), firstPBA)

	for _, e := range sm.Entries {
		if e.SSETPBA == uint64(storedPBA) {
			fmt.Printf("SetMap entry: name=%q setnum=%d SSETPBA=%d\n", e.Name, e.SetNumber, e.SSETPBA)
		}
	}

	probe := func(label string, pba int64) (*mtf.Block, error) {
		if err := rc.Rewind(); err != nil {
			return nil, err
		}
		r := mtf.NewReader(rc)
		if _, err := r.Next(); err != nil {
			return nil, fmt.Errorf("read TAPE: %w", err)
		}
		if err := r.SeekToBlock(pba); err != nil {
			return nil, fmt.Errorf("seek: %w", err)
		}
		return r.Next()
	}

	candidates := []struct {
		pba   int64
		label string
	}{
		{firstPBA - 1, "first-1"},
		{firstPBA - 2, "first-2"},
		{firstPBA, "first+0"},
		{firstPBA - 3, "first-3"},
		{firstPBA + 1, "first+1"},
	}

	fmt.Println("\n=== Probing first SSET ===")
	for _, c := range candidates {
		if c.pba < 0 {
			continue
		}
		fmt.Printf("Probing PBA %d (%s)\n", c.pba, c.label)
		blk, err := probe("", c.pba)
		if err != nil {
			fmt.Printf("  error: %v\n", err)
			continue
		}
		fmt.Printf("  kind=%d", blk.Kind)
		if blk.Kind == mtf.KindSet {
			offset := firstPBA - c.pba
			fmt.Printf(" KindSet(number=%d name=%q offset=%d)", blk.Set.Number, blk.Set.Name, offset)
		}
		fmt.Println()
		if blk.Kind == mtf.KindSet {
			fmt.Printf("FIRST-SSET: actual=%d stored=%d offset=%d\n", c.pba, firstPBA, firstPBA-c.pba)
			break
		}
	}

	candidates2 := []struct {
		pba   int64
		label string
	}{
		{storedPBA - 2, "ds4-2"},
		{storedPBA - 1, "ds4-1"},
		{storedPBA, "ds4+0"},
		{storedPBA + 1, "ds4+1"},
		{storedPBA - 3, "ds4-3"},
		{storedPBA + 2, "ds4+2"},
		{storedPBA - 4, "ds4-4"},
		{storedPBA + 3, "ds4+3"},
	}

	fmt.Printf("\n=== Probing dataset 4 SSETPBA=%d ===\n", storedPBA)
	for _, c := range candidates2 {
		if c.pba < 0 {
			continue
		}
		fmt.Printf("Probing PBA %d (%s)\n", c.pba, c.label)
		blk, err := probe("", c.pba)
		if err != nil {
			fmt.Printf("  error: %v\n", err)
			continue
		}
		fmt.Printf("  kind=%d", blk.Kind)
		if blk.Kind == mtf.KindSet {
			fmt.Printf(" KindSet(number=%d name=%q)", blk.Set.Number, blk.Set.Name)
		}
		fmt.Println()
		if blk.Kind == mtf.KindSet {
			offset := storedPBA - c.pba
			fmt.Printf("DS4-SSET: actual=%d stored=%d offset=%d\n", c.pba, storedPBA, offset)
			break
		}
	}
}

func streamIDStr(id uint32) string {
	b := [4]byte{
		byte(id & 0xff),
		byte((id >> 8) & 0xff),
		byte((id >> 16) & 0xff),
		byte((id >> 24) & 0xff),
	}
	return string(b[:])
}

func walkSSETMain(rc *tapeio.TapeReader, r *mtf.Reader) {
	fmt.Println("Walking tape sequentially, reporting SSET positions...")
	sm, err := mtf.ReadSetMap(rc)
	if err != nil {
		die(fmt.Errorf("ReadSetMap: %w", err))
	}
	fmt.Printf("SetMap: %d entries\n", len(sm.Entries))
	for i, e := range sm.Entries {
		fmt.Printf("  entry[%d]: name=%q setnum=%d SSETPBA=%d\n", i, e.Name, e.SetNumber, e.SSETPBA)
	}

	if err := rc.Rewind(); err != nil {
		die(err)
	}
	r = mtf.NewReader(rc)
	nBlk := 0
	nSSET := 0
	for {
		blk, err := r.Next()
		if err != nil {
			fmt.Printf("\n(end: %v after %d blocks, %d SSETs)\n", err, nBlk, nSSET)
			return
		}
		nBlk++
		switch blk.Kind {
		case mtf.KindMedia:
			pos, errPos := rc.TellBlock()
			fmt.Printf("%4d MEDIA  flb=%d pos=%d\n", nBlk, blk.Tape.FLBSize, pos)
			if errPos != nil {
				fmt.Printf("  TellBlock error: %v\n", errPos)
			}
		case mtf.KindSet:
			nSSET++
			pos, errPos := rc.TellBlock()
			var storedPBA int64
			for i := range sm.Entries {
				if int(sm.Entries[i].SetNumber) == int(blk.Set.Number) {
					storedPBA = int64(sm.Entries[i].SSETPBA)
					break
				}
			}
			offset := storedPBA - pos
			fmt.Printf("%4d SSET#%d num=%d name=%q pos=%d storedPBA=%d offset=%d\n",
				nBlk, nSSET, blk.Set.Number, blk.Set.Name, pos, storedPBA, offset)
			if errPos != nil {
				fmt.Printf("  TellBlock error: %v\n", errPos)
			}
		case mtf.KindSetEnd:
			pos, errPos := rc.TellBlock()
			fmt.Printf("%4d SETEND pos=%d\n", nBlk, pos)
			if errPos != nil {
				fmt.Printf("  TellBlock error: %v\n", errPos)
			}
		default:
		}
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
