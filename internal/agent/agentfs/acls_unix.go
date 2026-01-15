//go:build unix

package agentfs

import (
	"encoding/binary"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const (
	ACL_UNDEFINED_TAG = 0x00
	ACL_USER_OBJ      = 0x01
	ACL_USER          = 0x02
	ACL_GROUP_OBJ     = 0x04
	ACL_GROUP         = 0x08
	ACL_MASK          = 0x10
	ACL_OTHER         = 0x20

	XATTR_NAME_ACL_ACCESS  = "system.posix_acl_access"
	XATTR_NAME_ACL_DEFAULT = "system.posix_acl_default"

	ACL_EA_VERSION = 0x0002
)

func GetUnixACLs(path string, fd int) ([]types.PosixACL, error) {
	access, err := getACL(path, fd, XATTR_NAME_ACL_ACCESS)
	if err != nil && !isNoAttr(err) {
		return nil, err
	}

	defaultAcl, err := getACL(path, fd, XATTR_NAME_ACL_DEFAULT)
	if err != nil && !isNoAttr(err) {
		return access, nil
	}

	if len(defaultAcl) > 0 {
		access = append(access, defaultAcl...)
	}

	return access, nil
}

func getACL(path string, fd int, attr string) ([]types.PosixACL, error) {
	var size int
	var err error
	if fd > 0 {
		size, err = unix.Fgetxattr(fd, attr, nil)
	} else {
		size, err = unix.Lgetxattr(path, attr, nil)
	}

	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	if fd > 0 {
		_, err = unix.Fgetxattr(fd, attr, buf)
	} else {
		_, err = unix.Lgetxattr(path, attr, buf)
	}
	if err != nil {
		return nil, err
	}

	return parseUnixACL(buf, attr)
}

func parseUnixACL(buf []byte, attr string) ([]types.PosixACL, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("ACL too short")
	}

	version := binary.LittleEndian.Uint32(buf[0:4])
	if version != ACL_EA_VERSION {
		return nil, fmt.Errorf("unsupported ACL version: %d", version)
	}

	// Each entry is 8 bytes: tag (2), perms (2), id (4)
	buf = buf[4:]
	if len(buf)%8 != 0 {
		return nil, fmt.Errorf("malformed ACL entries")
	}

	count := len(buf) / 8
	result := make([]types.PosixACL, 0, count)

	for i := 0; i < count; i++ {
		off := i * 8
		tag := binary.LittleEndian.Uint16(buf[off : off+2])
		perms := binary.LittleEndian.Uint16(buf[off+2 : off+4])
		id := int32(binary.LittleEndian.Uint32(buf[off+4 : off+8]))

		tagStr := ""
		switch tag {
		case ACL_USER_OBJ:
			tagStr = "user_obj"
		case ACL_USER:
			tagStr = "user"
		case ACL_GROUP_OBJ:
			tagStr = "group_obj"
		case ACL_GROUP:
			tagStr = "group"
		case ACL_MASK:
			tagStr = "mask"
		case ACL_OTHER:
			tagStr = "other"
		default:
			continue // Skip undefined tags
		}

		result = append(result, types.PosixACL{
			Tag:       tagStr,
			ID:        id,
			Perms:     uint8(perms),
			IsDefault: attr == XATTR_NAME_ACL_DEFAULT,
		})
	}

	return result, nil
}
