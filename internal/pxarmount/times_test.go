package pxarmount

import (
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// TestResolvePxarTimesMatchesRestore pins the contract that the pxar-mount
// reports the same atime/mtime restore would set, given identical pxar metadata.
// restore_unix.go applyMeta defaults both times to Stat.Mtime, then overrides
// atime from user.lastaccesstime and mtime from user.lastwritetime (decimal
// Unix seconds, nanos dropped) when those xattrs are present and valid.
func TestResolvePxarTimesMatchesRestore(t *testing.T) {
	const statSecs int64 = 1_700_000_000
	const statNanos uint32 = 123_456_789
	const accSecs int64 = 1_700_000_100 // user.lastaccesstime
	const wrSecs int64 = 1_700_000_200  // user.lastwritetime

	mk := func(xas ...format.XAttr) *pxar.Entry {
		return &pxar.Entry{
			Metadata: pxar.Metadata{
				Stat:   format.Stat{Mtime: format.StatxTimestamp{Secs: statSecs, Nanos: statNanos}},
				XAttrs: xas,
			},
		}
	}

	tests := []struct {
		name        string
		entry       *pxar.Entry
		wantAtimeNs int64
		wantMtimeNs int64
	}{
		{
			name:        "no xattrs -> both default to Stat.Mtime (with nanos)",
			entry:       mk(),
			wantAtimeNs: statSecs*1e9 + int64(statNanos),
			wantMtimeNs: statSecs*1e9 + int64(statNanos),
		},
		{
			name: "both xattrs -> atime/mtime from xattrs, nanos dropped",
			entry: mk(
				format.NewXAttr([]byte("user.lastaccesstime"), []byte("1700000100")),
				format.NewXAttr([]byte("user.lastwritetime"), []byte("1700000200")),
			),
			wantAtimeNs: accSecs * 1e9,
			wantMtimeNs: wrSecs * 1e9,
		},
		{
			name: "only lastwritetime -> mtime overridden, atime stays Stat.Mtime",
			entry: mk(
				format.NewXAttr([]byte("user.lastwritetime"), []byte("1700000200")),
			),
			wantAtimeNs: statSecs*1e9 + int64(statNanos),
			wantMtimeNs: wrSecs * 1e9,
		},
		{
			name: "only lastaccesstime -> atime overridden, mtime stays Stat.Mtime",
			entry: mk(
				format.NewXAttr([]byte("user.lastaccesstime"), []byte("1700000100")),
			),
			wantAtimeNs: accSecs * 1e9,
			wantMtimeNs: statSecs*1e9 + int64(statNanos),
		},
		{
			name: "malformed xattrs -> fall back to Stat.Mtime",
			entry: mk(
				format.NewXAttr([]byte("user.lastaccesstime"), []byte("not-a-number")),
				format.NewXAttr([]byte("user.lastwritetime"), []byte("")),
			),
			wantAtimeNs: statSecs*1e9 + int64(statNanos),
			wantMtimeNs: statSecs*1e9 + int64(statNanos),
		},
		{
			name: "garbage (old binary-decode range) xattrs rejected",
			entry: mk(
				format.NewXAttr([]byte("user.lastwritetime"), []byte("36028797018963968")),
			),
			wantAtimeNs: statSecs*1e9 + int64(statNanos),
			wantMtimeNs: statSecs*1e9 + int64(statNanos),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAtimeNs, gotMtimeNs := resolvePxarTimes(tt.entry)
			if gotAtimeNs != tt.wantAtimeNs {
				t.Errorf("atimeNs = %d, want %d", gotAtimeNs, tt.wantAtimeNs)
			}
			if gotMtimeNs != tt.wantMtimeNs {
				t.Errorf("mtimeNs = %d, want %d", gotMtimeNs, tt.wantMtimeNs)
			}
		})
	}
}

// TestParseXattrUnixSecsLocal pins the decimal-Unix-seconds decoding shared
// with restore. Mirrors internal/pxar TestParseXattrUnixSecs so a drift in
// either copy is caught.
func TestParseXattrUnixSecsLocal(t *testing.T) {
	tests := []struct {
		in     string
		want   int64
		wantOk bool
	}{
		{"1700000200", 1700000200, true},
		{"0", 0, true},
		{"", 0, false},
		{"not-a-number", 0, false},
		{"-1", 0, false},
		{"36028797018963968", 0, false}, // garbage from old binary decode
	}
	for _, tt := range tests {
		got, ok := parseXattrUnixSecsLocal([]byte(tt.in))
		if ok != tt.wantOk {
			t.Errorf("parseXattrUnixSecsLocal(%q) ok=%v want %v", tt.in, ok, tt.wantOk)
		}
		if ok && got != tt.want {
			t.Errorf("parseXattrUnixSecsLocal(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
