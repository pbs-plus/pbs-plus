package tapeio

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func mtfToPxarMeta(h *mtf.Header, fileType uint64, fallbackTime time.Time) pxar.Metadata {
	var m pxar.Metadata
	m.Stat.Mode = fileType | (uint64(h.UnixMode()) &^ format.ModeIFMT)

	modTime := h.ModTime
	if modTime.IsZero() {
		if !h.CreateTime.IsZero() {
			modTime = h.CreateTime
		} else if !h.AccessTime.IsZero() {
			modTime = h.AccessTime
		} else if !h.BirthTime.IsZero() {
			modTime = h.BirthTime
		} else if !fallbackTime.IsZero() {
			modTime = fallbackTime
		}
	}
	m.Stat.Mtime = format.NewStatxTimestampFromTime(modTime)

	if !h.ModTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.lastwritetime"), []byte(strconv.FormatInt(h.ModTime.Unix(), 10))))
	}
	if !h.CreateTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.creationtime"), []byte(strconv.FormatInt(h.CreateTime.Unix(), 10))))
	}
	if !h.AccessTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.lastaccesstime"), []byte(strconv.FormatInt(h.AccessTime.Unix(), 10))))
	}

	if len(h.SecurityDescriptor) > 0 {
		if ownerSID := h.OwnerSID(); ownerSID != nil {
			m.Stat.UID, m.Stat.GID = mapSID(mtf.FormatSID(ownerSID))
		}
	}

	if len(h.ExtendedAttributes) > 0 {
		if xattrs := parseNTEA(h.ExtendedAttributes); len(xattrs) > 0 {
			m.XAttrs = append(m.XAttrs, xattrs...)
		}
	}

	return m
}

func mapSID(sid string) (uid, gid uint32) {
	switch sid {
	case "S-1-5-18", "S-1-5-19", "S-1-5-20", "S-1-5-32-544":
		return 0, 0
	case "S-1-5-32-545":
		return 1000, 1000
	default:
		parts := strings.Split(sid, "-")
		if len(parts) >= 3 {
			var n uint32
			if _, err := fmt.Sscanf(parts[len(parts)-1], "%d", &n); err != nil {
				log.Error(err, "")
			}
			if n > 0 {
				return n + 1000, n + 1000
			}
		}
		return 0, 0
	}
}

func parseNTEA(data []byte) []format.XAttr {
	if len(data) < 4 {
		return nil
	}
	count := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
	if count == 0 || count > 256 {
		return nil
	}
	off := 4
	var xattrs []format.XAttr
	for i := 0; i < count && off+4 <= len(data); i++ {
		nameLen := int(uint16(data[off])|uint16(data[off+1])<<8) * 2
		valueLen := int(uint16(data[off+2]) | uint16(data[off+3])<<8)
		off += 4
		if off+nameLen > len(data) {
			break
		}
		nameBytes := data[off : off+nameLen]
		off += nameLen
		if off+valueLen > len(data) {
			break
		}
		valueBytes := data[off : off+valueLen]
		off += valueLen

		name := decodeUTF16LE(nameBytes)
		if name != "" {
			xattrs = append(xattrs, format.NewXAttr([]byte("user.ntea."+name), valueBytes))
		}
	}
	return xattrs
}

func decodeUTF16LE(data []byte) string {
	if len(data)%2 != 0 {
		return ""
	}
	runes := make([]rune, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		r := rune(uint16(data[i]) | uint16(data[i+1])<<8)
		if r == 0 {
			break
		}
		runes = append(runes, r)
	}
	return string(runes)
}
