package tapeio

import "testing"

func TestBarcodeLTOGen(t *testing.T) {
	cases := map[string]int{
		"ABC123L5": 5,
		"ABC123L6": 6,
		"ABC123L7": 7,
		"ABC123L8": 8,
		"ABC123L9": 9,
		"ABC123M8": 8,
		"M8":       8,
		"l8":       8,
		"ABC123":   ltUndefined,
		"":         ltUndefined,
		"CLN000":   ltUndefined,
	}
	for bc, want := range cases {
		if got := barcodeLTOGen(bc); got != want {
			t.Errorf("barcodeLTOGen(%q) = %d, want %d", bc, got, want)
		}
	}
}

func TestProductLTOGen(t *testing.T) {
	cases := map[string]int{
		"Ultrium 6":       6,
		"ULTRIUM 8":       8,
		"Ultrium-7":       7,
		"ULT3580-TD6":     6,
		"IBM-ULT3580-TD5": 5,
		"Something L9":    9,
		"T10000C":         ltUndefined,
		"":                ltUndefined,
	}
	for prod, want := range cases {
		if got := productLTOGen(prod); got != want {
			t.Errorf("productLTOGen(%q) = %d, want %d", prod, got, want)
		}
	}
}

func TestLTOReadCompatible(t *testing.T) {
	cases := []struct {
		drive, tape int
		want        bool
	}{
		{6, 6, true},
		{6, 5, true},
		{6, 7, false},
		{6, 8, false},
		{8, 8, true},
		{8, 7, true},
		{8, 6, false},
		{8, 5, false},
		{ltUndefined, 8, true},
		{6, ltUndefined, true},
	}
	for _, c := range cases {
		if got := ltoReadCompatible(c.drive, c.tape); got != c.want {
			t.Errorf("ltoReadCompatible(drive=%d, tape=%d) = %v, want %v", c.drive, c.tape, got, c.want)
		}
	}
}
