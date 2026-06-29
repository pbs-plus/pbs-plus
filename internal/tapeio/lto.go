package tapeio

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/pbs-plus/go-tapedrive"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const ltUndefined = 0

func ltoGenCandidate(driveGen, tapeGen int) bool {
	if driveGen == ltUndefined || tapeGen == ltUndefined {
		return true
	}
	return tapeGen <= driveGen
}

func barcodeLTOGen(barcode string) int {
	bc := strings.ToUpper(strings.TrimSpace(barcode))
	if bc == "M8" || strings.HasSuffix(bc, "M8") {
		return 8
	}
	for i := 0; i+1 < len(bc); i++ {
		if bc[i] == 'L' {
			switch bc[i+1] {
			case '5':
				return 5
			case '6':
				return 6
			case '7':
				return 7
			case '8':
				return 8
			case '9':
				return 9
			}
		}
	}
	return ltUndefined
}

func productLTOGen(product string) int {
	s := strings.ToUpper(product)

	if _, after, ok := strings.Cut(s, "ULTRIUM"); ok {
		if g := firstTrailingDigit(after); g > 0 {
			return g
		}
	}
	if strings.Contains(s, "ULT") {
		if i := strings.Index(s, "TD"); i >= 0 && i+2 < len(s) {
			if g := digitVal(s[i+2]); g > 0 {
				return g
			}
		}
	}
	for i := 0; i+1 < len(s); i++ {
		if s[i] == 'L' {
			if g := digitVal(s[i+1]); g >= 5 && g <= 9 {
				return g
			}
		}
	}
	return ltUndefined
}

func firstTrailingDigit(s string) int {
	s = strings.TrimLeftFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || r == '-' || r == '_'
	})
	if len(s) == 0 {
		return ltUndefined
	}
	return digitVal(s[0])
}

func digitVal(b byte) int {
	if b >= '0' && b <= '9' {
		return int(b - '0')
	}
	return ltUndefined
}

func detectDriveGen(tapeDev string) int {
	for _, p := range sgProbePaths(tapeDev) {
		if gen := tryInquireGen(p); gen != ltUndefined || p == tapeDev {
			return gen
		}
	}
	return ltUndefined
}

func tryInquireGen(tapeDev string) int {
	d, err := tapedrive.Open(tapeDev)
	if err != nil {
		log.Error(err, "lto: cannot open drive for INQUIRY", "device", tapeDev)
		return ltUndefined
	}
	defer func() {
		if err := d.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	in, err := d.Inquiry()
	if err != nil {
		log.Error(err, "lto: INQUIRY failed", "device", tapeDev)
		return ltUndefined
	}
	gen := productLTOGen(in.Product)
	if gen == ltUndefined {
		log.Info("lto: could not derive drive generation from product; assuming read-compatible with all tapes",
			"vendor", in.Vendor, "product", in.Product)
	} else {
		log.Info("lto: detected drive generation",
			"vendor", in.Vendor, "product", in.Product, "generation", gen)
	}
	return gen
}

func sgProbePaths(tapeDev string) []string {
	if before, ok := strings.CutSuffix(tapeDev, "-nst"); ok {
		sg := before + "-sg"
		if _, err := os.Stat(sg); err == nil {
			return []string{sg, tapeDev}
		}
	}
	return []string{tapeDev}
}

func _genDesc(gen int) string {
	if gen == ltUndefined {
		return "unknown LTO generation"
	}
	return fmt.Sprintf("LTO-%d", gen)
}

func ProbeDriveGen(tapeDev string) int { return detectDriveGen(tapeDev) }

func ProbeBarcodeGen(barcode string) int { return barcodeLTOGen(barcode) }
