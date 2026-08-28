package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

type ACLConfig struct {
	OwnerUID int
	OwnerGID int

	// Full POSIX ACL entries. When set, these are served as virtual
	// system.posix_acl_access / system.posix_acl_default xattrs.
	ACLEntries        []ACLEntry
	DefaultACLEntries []ACLEntry
}

type ACLEntry struct {
	Tag  uint16 // ACL_USER_OBJ, ACL_USER, ACL_GROUP_OBJ, ACL_GROUP, ACL_MASK, ACL_OTHER
	Perm uint16 // permission bits (r=4, w=2, x=1)
	ID   uint32 // UID or GID for ACL_USER / ACL_GROUP entries
}

// POSIX ACL tag constants (matching Linux kernel definitions).

// MarshalACL encodes POSIX ACL entries into the kernel binary format
// used by system.posix_acl_access and system.posix_acl_default.
func MarshalACL(entries []ACLEntry) []byte {
	buf := make([]byte, 4+len(entries)*8)
	binary.LittleEndian.PutUint32(buf[:4], ACLXAttrVersion)
	for i, e := range entries {
		off := 4 + i*8
		binary.LittleEndian.PutUint16(buf[off:off+2], e.Tag)
		binary.LittleEndian.PutUint16(buf[off+2:off+4], e.Perm)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], e.ID)
	}
	return buf
}

// user:backupadmin:rwx

// user:backupadmin:rwx
func ParseACLSpec(spec string) ([]ACLEntry, error) {
	var entries []ACLEntry
	// Accept both \n and ; as delimiters.
	spec = strings.ReplaceAll(spec, ";", "\n")
	for line := range strings.SplitSeq(spec, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		e, err := parseACLEntry(line)
		if err != nil {
			return nil, fmt.Errorf("parse ACL line %q: %w", line, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func parseACLEntry(line string) (ACLEntry, error) {
	parts := strings.SplitN(line, ":", 3)
	if len(parts) < 3 {
		return ACLEntry{}, fmt.Errorf("invalid format")
	}
	kind, name, perm := parts[0], parts[1], parts[2]

	var e ACLEntry
	switch kind {
	case "user":
		if name == "" {
			e.Tag = ACLUserObj
		} else {
			e.Tag = ACLUser
			uid, err := lookupUID(name)
			if err != nil {
				return ACLEntry{}, fmt.Errorf("unknown user %q: %w", name, err)
			}
			e.ID = uid
		}
	case "group":
		if name == "" {
			e.Tag = ACLGroupObj
		} else {
			e.Tag = ACLGroup
			gid, err := lookupGID(name)
			if err != nil {
				return ACLEntry{}, fmt.Errorf("unknown group %q: %w", name, err)
			}
			e.ID = gid
		}
	case "mask":
		e.Tag = ACLMask
	case "other":
		e.Tag = ACLOther
	default:
		return ACLEntry{}, fmt.Errorf("unknown ACL type %q", kind)
	}

	e.Perm = parsePerm(perm)
	return e, nil
}

func parsePerm(s string) uint16 {
	var p uint16
	for _, c := range s {
		switch c {
		case 'r':
			p |= 4
		case 'w':
			p |= 2
		case 'x':
			p |= 1
		}
	}
	return p
}

func lookupUID(name string) (uint32, error) {
	// Try Go's user.Lookup first (uses NSS when dynamically linked).
	if u, err := user.Lookup(name); err == nil {
		uid, err := strconv.ParseUint(u.Uid, 10, 32)
		if err != nil {
			log.Error(err, "")
		}
		return uint32(uid), nil
	}
	// Fallback: try getent which respects NSS/winbind even from
	out, err := exec.Command("getent", "passwd", name).Output()
	if err != nil {
		return 0, fmt.Errorf("unknown user %q", name)
	}
	fields := strings.SplitN(strings.TrimSpace(string(out)), ":", 4)
	if len(fields) < 3 {
		return 0, fmt.Errorf("malformed getent output for user %q", name)
	}
	uid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad uid for user %q: %w", name, err)
	}
	return uint32(uid), nil
}

func lookupGID(name string) (uint32, error) {
	if g, err := user.LookupGroup(name); err == nil {
		gid, err := strconv.ParseUint(g.Gid, 10, 32)
		if err != nil {
			log.Error(err, "")
		}
		return uint32(gid), nil
	}
	out, err := exec.Command("getent", "group", name).Output()
	if err != nil {
		return 0, fmt.Errorf("unknown group %q", name)
	}
	fields := strings.SplitN(strings.TrimSpace(string(out)), ":", 4)
	if len(fields) < 3 {
		return 0, fmt.Errorf("malformed getent output for group %q", name)
	}
	gid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad gid for group %q: %w", name, err)
	}
	return uint32(gid), nil
}

func (c ACLConfig) HasACLs() bool {
	return len(c.ACLEntries) > 0
}

func BuildACLConfig(ownerUID, ownerGID int, aclSpec, defaultAclSpec string) ACLConfig {
	cfg := ACLConfig{
		OwnerUID: ownerUID,
		OwnerGID: ownerGID,
	}
	if aclSpec != "" {
		entries, err := ParseACLSpec(aclSpec)
		if err != nil {
			log.Error(err, "error parsing acl-spec")
			os.Exit(1)
		}
		cfg.ACLEntries = entries
	}
	if defaultAclSpec != "" {
		entries, err := ParseACLSpec(defaultAclSpec)
		if err != nil {
			log.Error(err, "error parsing default-acl-spec")
			os.Exit(1)
		}
		cfg.DefaultACLEntries = entries
	}
	return cfg
}
