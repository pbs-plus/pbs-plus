package tapeio

import (
	"strconv"
	"testing"
	"time"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pxar/format"
)

func TestMtfToPxarMeta_ModTimeFallback(t *testing.T) {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.Local)
	fallback := time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local)

	tests := []struct {
		name         string
		modTime      time.Time
		createTime   time.Time
		accessTime   time.Time
		birthTime    time.Time
		fallbackTime time.Time
		wantMtimeSec int64
		wantXattrW   bool
	}{
		{
			name:         "modtime present",
			modTime:      ts,
			createTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local),
			fallbackTime: fallback,
			wantMtimeSec: ts.Unix(),
			wantXattrW:   true,
		},
		{
			name:         "modtime zero falls back to createtime",
			modTime:      time.Time{},
			createTime:   ts,
			fallbackTime: fallback,
			wantMtimeSec: ts.Unix(),
			wantXattrW:   false,
		},
		{
			name:         "modtime and createtime zero falls back to accesstime",
			modTime:      time.Time{},
			createTime:   time.Time{},
			accessTime:   ts,
			fallbackTime: fallback,
			wantMtimeSec: ts.Unix(),
			wantXattrW:   false,
		},
		{
			name:         "all zero falls back to birthtime",
			modTime:      time.Time{},
			createTime:   time.Time{},
			accessTime:   time.Time{},
			birthTime:    ts,
			fallbackTime: fallback,
			wantMtimeSec: ts.Unix(),
			wantXattrW:   false,
		},
		{
			name:         "all zero falls back to backup time",
			modTime:      time.Time{},
			createTime:   time.Time{},
			accessTime:   time.Time{},
			birthTime:    time.Time{},
			fallbackTime: fallback,
			wantMtimeSec: fallback.Unix(),
			wantXattrW:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &mtf.Header{
				ModTime:    tt.modTime,
				CreateTime: tt.createTime,
				AccessTime: tt.accessTime,
				BirthTime:  tt.birthTime,
			}
			meta := mtfToPxarMeta(h, format.ModeIFREG, tt.fallbackTime)
			if meta.Stat.Mtime.Secs != tt.wantMtimeSec {
				t.Errorf("Mtime.Secs = %d, want %d", meta.Stat.Mtime.Secs, tt.wantMtimeSec)
			}

			foundW := false
			for _, xa := range meta.XAttrs {
				switch string(xa.Name()) {
				case "user.lastwritetime":
					if !tt.wantXattrW {
						t.Error("unexpected user.lastwritetime xattr")
					}
					foundW = true
					if v, _ := strconv.ParseInt(string(xa.Value()), 10, 64); v != tt.modTime.Unix() && tt.wantXattrW {
						t.Errorf("user.lastwritetime = %d, want %d", v, tt.modTime.Unix())
					}
				}
			}
			if tt.wantXattrW && !foundW {
				t.Error("missing user.lastwritetime xattr")
			}
		})
	}
}
