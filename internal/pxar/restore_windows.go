//go:build windows

package pxar

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	pxar "github.com/pbs-plus/pxar"
	"golang.org/x/sys/windows"
)

var (
	modAdvapi32          = syscall.NewLazyDLL("advapi32.dll")
	modKernel32          = syscall.NewLazyDLL("kernel32.dll")
	procSetEntriesInAclW = modAdvapi32.NewProc("SetEntriesInAclW")
	procSetSecurityInfo  = modAdvapi32.NewProc("SetSecurityInfo")
	procLocalFree        = modKernel32.NewProc("LocalFree")
)

type FILE_BASIC_INFO struct {
	CreationTime   windows.Filetime
	LastAccessTime windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	FileAttributes uint32
}

func applyMeta(ctx context.Context, st *restoreState, file *os.File, e pxar.FileInfo) error {
	defer file.Close()

	h := windows.Handle(file.Fd())
	path := file.Name()

	xattrs, lerr := st.client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if lerr != nil {
		st.reportErr(ctx, "list xattrs", path, lerr)
	}

	baseFt := unixToFiletime(e.MtimeSecs)
	var c windows.Filetime
	a := baseFt
	w := baseFt
	var hasCreation bool

	if xattrs != nil {
		if d, ok := xattrs[XAttrCreationTime]; ok {
			if ts, ok := ParseXattrUnixSecs(d); ok {
				c = unixToFiletime(ts)
				hasCreation = true
			}
		}
		if d, ok := xattrs[XAttrLastWriteTime]; ok {
			if ts, ok := ParseXattrUnixSecs(d); ok {
				w = unixToFiletime(ts)
			}
		}
		if d, ok := xattrs[XAttrLastAccessTime]; ok {
			if ts, ok := ParseXattrUnixSecs(d); ok {
				a = unixToFiletime(ts)
			}
		}
	}

	var attrs uint32
	var hasAttrs bool
	if xattrs != nil && st.fsCap.supportsXAttrs {
		if d, ok := xattrs[XAttrFileAttributes]; ok {
			var fa map[string]bool
			if cbor.Unmarshal(d, &fa) == nil {
				if attr := buildFileAttributes(fa); attr != 0 {
					attrs = attr
					hasAttrs = true
				}
			}
		}
	}

	if hasAttrs {
		info := FILE_BASIC_INFO{
			CreationTime:   c,
			LastAccessTime: a,
			LastWriteTime:  w,
			FileAttributes: attrs,
		}
		st.reportErr(ctx, "set basic info", path, setBasicInfo(h, &info))
	} else {
		var cp *windows.Filetime
		if hasCreation {
			cp = &c
		}
		st.reportErr(ctx, "set file time", path, windows.SetFileTime(h, cp, &a, &w))
	}

	if xattrs != nil {
		// WRITE_DAC/WRITE_OWNER, which normally causes SetSecurityInfo to
		if st.fsCap.supportsPersistentACLs {
			restoreWindowsACLsFromPath(ctx, st, path, xattrs)
		}
		// Restore non-canonical xattrs as NTFS alternate data streams
		// (canonical keys consumed above are skipped).
		if st.fsCap.supportsXAttrs {
			writeAlternateDataStreams(ctx, st, path, xattrs)
		}
	}

	return nil
}

func setBasicInfo(h windows.Handle, info *FILE_BASIC_INFO) error {
	return windows.SetFileInformationByHandle(h, windows.FileBasicInfo, (*byte)(unsafe.Pointer(info)), uint32(unsafe.Sizeof(*info)))
}

// restoreWindowsACLsFromPath opens the file with WRITE_DAC|WRITE_OWNER and
// FILE_FLAG_BACKUP_SEMANTICS, then delegates to restoreWindowsACLsFromHandle.
// SIDs are intentionally leaked — see restoreWindowsACLsFromHandle.
func restoreWindowsACLsFromPath(ctx context.Context, st *restoreState, path string, xattrs map[string][]byte) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		st.reportErr(ctx, "open for acls", path, err)
		return
	}
	h, err := windows.CreateFile(
		pathPtr,
		windows.WRITE_DAC|windows.WRITE_OWNER,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		st.reportErr(ctx, "open for acls", path, err)
		return
	}
	defer windows.CloseHandle(h)
	restoreWindowsACLsFromHandle(ctx, st, h, path, xattrs)
}

// restoreWindowsACLsFromHandle applies owner, group, and DACL via SetSecurityInfo.
// SIDs allocated by StringToSid are intentionally NOT freed: on Windows build
// 10.0.26200 LocalFree on a ConvertStringSidToSidW SID causes heap corruption
// (0xC0000374). The leak is bounded (freed at process exit).
func restoreWindowsACLsFromHandle(ctx context.Context, st *restoreState, h windows.Handle, path string, xattrs map[string][]byte) {
	var secInfo windows.SECURITY_INFORMATION
	var ownerSID, groupSID *windows.SID

	if d, ok := xattrs["user.owner"]; ok {
		if sid, err := windows.StringToSid(string(d)); err == nil {
			ownerSID = sid
			secInfo |= windows.OWNER_SECURITY_INFORMATION
		}
	}
	if d, ok := xattrs["user.group"]; ok {
		if sid, err := windows.StringToSid(string(d)); err == nil {
			groupSID = sid
			secInfo |= windows.GROUP_SECURITY_INFORMATION
		}
	}

	var dacl *windows.ACL
	if d, ok := xattrs["user.acls"]; ok {
		if detectACLFlavor(d) == aclWindows {
			var winACLs []types.WinACL
			if uerr := cbor.Unmarshal(d, &winACLs); uerr != nil {
				st.reportErr(ctx, "decode acls", path, uerr)
			} else {
				acl, _, berr := buildDACLFromACEs(winACLs)
				if berr != nil {
					st.reportErr(ctx, "build dacl", path, berr)
				} else if acl != nil {
					dacl = acl
					secInfo |= windows.DACL_SECURITY_INFORMATION
				}
			}
		}
	}

	if secInfo == 0 || h == 0 || h == windows.InvalidHandle {
		return
	}

	ret, _, _ := procSetSecurityInfo.Call(
		uintptr(h), uintptr(windows.SE_FILE_OBJECT), uintptr(secInfo),
		sidPtr(ownerSID), sidPtr(groupSID), aclPtr(dacl), 0,
	)
	if ret != 0 {
		st.reportErr(ctx, "set security info", path, syscall.Errno(ret))
	}

	localFreePtr(aclPtr(dacl))
}

func applyMetaSymlink(ctx context.Context, st *restoreState, linkPath string, e pxar.FileInfo) error {
	h, err := openReparsePoint(linkPath, windows.FILE_WRITE_ATTRIBUTES|windows.WRITE_DAC|windows.WRITE_OWNER)
	if err != nil {
		st.reportErr(ctx, "open symlink for metadata", linkPath, err)
		return nil
	}
	defer windows.CloseHandle(h)

	xattrs, lerr := st.client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if lerr != nil {
		st.reportErr(ctx, "list xattrs", linkPath, lerr)
	}
	if xattrs == nil {
		return nil
	}

	// File attributes are NOT set on a reparse point — writing them could
	// clear FILE_ATTRIBUTE_REPARSE_POINT and break the symlink.
	baseFt := unixToFiletime(e.MtimeSecs)
	a := baseFt
	w := baseFt
	var cp *windows.Filetime
	if d, ok := xattrs[XAttrCreationTime]; ok {
		if ts, ok := ParseXattrUnixSecs(d); ok {
			ft := unixToFiletime(ts)
			cp = &ft
		}
	}
	if d, ok := xattrs[XAttrLastWriteTime]; ok {
		if ts, ok := ParseXattrUnixSecs(d); ok {
			w = unixToFiletime(ts)
		}
	}
	if d, ok := xattrs[XAttrLastAccessTime]; ok {
		if ts, ok := ParseXattrUnixSecs(d); ok {
			a = unixToFiletime(ts)
		}
	}
	st.reportErr(ctx, "set file time", linkPath, windows.SetFileTime(h, cp, &a, &w))

	if st.fsCap.supportsPersistentACLs {
		restoreWindowsACLsFromHandle(ctx, st, h, linkPath, xattrs)
	}
	return nil
}

func applyTempMode(path string, rawMode uint64) error { return nil }

func openReparsePoint(p string, access uint32) (windows.Handle, error) {
	ptr, err := windows.UTF16PtrFromString(p)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(
		ptr, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0,
	)
}

// writeAlternateDataStreams writes non-canonical user.* xattrs as NTFS
func writeAlternateDataStreams(ctx context.Context, st *restoreState, name string, xattrs map[string][]byte) {
	for k, v := range xattrs {
		if !strings.HasPrefix(k, "user.") {
			continue
		}
		if IsCanonicalXAttr(k) {
			continue
		}
		stream := strings.TrimPrefix(k, "user.")
		if stream == "" || strings.ContainsAny(stream, `<>:"/\|?*`) {
			continue
		}
		st.reportErr(ctx, "write ads "+k, name, os.WriteFile(name+":"+stream, v, 0o666))
	}
}

func unixToFiletime(unixTime int64) windows.Filetime {
	const ticksPerSecond = 10000000
	const epochDifference = 116444736000000000

	ticks := unixTime*ticksPerSecond + epochDifference
	return windows.Filetime{
		LowDateTime:  uint32(ticks & 0xFFFFFFFF),
		HighDateTime: uint32(ticks >> 32),
	}
}

func buildFileAttributes(fa map[string]bool) uint32 {
	var r uint32
	m := map[string]uint32{
		"FILE_ATTRIBUTE_READONLY": windows.FILE_ATTRIBUTE_READONLY,
		"FILE_ATTRIBUTE_HIDDEN":   windows.FILE_ATTRIBUTE_HIDDEN,
		"FILE_ATTRIBUTE_SYSTEM":   windows.FILE_ATTRIBUTE_SYSTEM,
		"FILE_ATTRIBUTE_ARCHIVE":  windows.FILE_ATTRIBUTE_ARCHIVE,
	}
	for k, v := range m {
		if fa[k] {
			r |= v
		}
	}
	return r
}

// buildDACLFromACEs builds a DACL from WinACL entries, returning allocated SIDs
// so the caller can free them after SetSecurityInfo consumes the ACL.
func buildDACLFromACEs(winACLs []types.WinACL) (acl *windows.ACL, sids []*windows.SID, err error) {
	if len(winACLs) == 0 {
		return nil, nil, nil
	}

	entries := make([]windows.EXPLICIT_ACCESS, 0, len(winACLs))

	for _, acl := range winACLs {
		sid, serr := windows.StringToSid(acl.SID)
		if serr != nil {
			continue
		}
		sids = append(sids, sid)
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: windows.ACCESS_MASK(acl.AccessMask),
			AccessMode:        windows.ACCESS_MODE(acl.Type),
			Inheritance:       uint32(acl.Flags),
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		})
	}

	if len(entries) == 0 {
		return nil, sids, nil
	}

	var newACL *windows.ACL
	ret, _, _ := procSetEntriesInAclW.Call(
		uintptr(len(entries)),
		uintptr(unsafe.Pointer(&entries[0])),
		0,
		uintptr(unsafe.Pointer(&newACL)),
	)
	if ret != 0 {
		return nil, sids, fmt.Errorf("SetEntriesInAcl failed: %w", syscall.Errno(ret))
	}
	return newACL, sids, nil
}

func restoreDir(ctx context.Context, st *restoreState, job restoreJob) error {
	dst := job.dest
	dirEntry := job.info
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := st.client.ReadDir(ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())
		childSrc := path.Join(job.srcPath, e.Name())

		st.wg.Add(1)
		go func(t, s string, info pxar.FileInfo) {
			select {
			case st.jobs <- restoreJob{dest: t, srcPath: s, info: info}:
			case <-ctx.Done():
				st.wg.Done()
			}
		}(target, childSrc, e)
	}

	if st.noAttr {
		return nil
	}

	pathPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}

	h, err := windows.CreateFile(
		pathPtr,
		windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}

	df := os.NewFile(uintptr(h), dst)
	return applyMeta(ctx, st, df, dirEntry)
}

func sidPtr(s *windows.SID) uintptr {
	if s == nil {
		return 0
	}
	return uintptr(unsafe.Pointer(s))
}

func aclPtr(a *windows.ACL) uintptr {
	if a == nil {
		return 0
	}
	return uintptr(unsafe.Pointer(a))
}

func localFreePtr(p uintptr) {
	if p == 0 {
		return
	}
	_, _, _ = procLocalFree.Call(p)
}
