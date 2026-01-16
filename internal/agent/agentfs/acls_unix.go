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
	access, err := getACL(path, fd, XATTR_NAME_ACL_ACCESS, false)
	if err != nil && !isNoAttr(err) {
		return nil, err
	}

	defaultAcl, err := getACL(path, fd, XATTR_NAME_ACL_DEFAULT, true)
	if err != nil && !isNoAttr(err) {
		return access, nil
	}

	return append(access, defaultAcl...), nil
}

func getACL(path string, fd int, attr string, isDefault bool) ([]types.PosixACL, error) {
	var size int
	var err error
	if fd > 0 {
		size, err = unix.Fgetxattr(fd, attr, nil)
	} else {
		size, err = unix.Lgetxattr(path, attr, nil)
	}

	if err != nil || size == 0 {
		return nil, err
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

	return parseUnixACL(buf, isDefault)
}

func parseUnixACL(buf []byte, isDefault bool) ([]types.PosixACL, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("ACL too short")
	}

	version := binary.LittleEndian.Uint32(buf[0:4])
	if version != ACL_EA_VERSION {
		return nil, fmt.Errorf("unsupported ACL version: %d", version)
	}

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

		var tagStr string
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
			continue
		}

		result = append(result, types.PosixACL{
			Tag:       tagStr,
			ID:        id,
			Perms:     uint8(perms),
			IsDefault: isDefault,
		})
	}

	return result, nil
}
