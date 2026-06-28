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

func streamIDStr(id uint32) string {
	b := [4]byte{
		byte(id & 0xff),
		byte((id >> 8) & 0xff),
		byte((id >> 16) & 0xff),
		byte((id >> 24) & 0xff),
	}
	return string(b[:])
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
