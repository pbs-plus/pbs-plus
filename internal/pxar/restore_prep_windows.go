//go:build windows

package pxar

import (
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

var prepOnce sync.Once

// prepareRestoreProcess enables the backup/restore/ownership privileges in the
// current process's token. These privileges are present (but disabled) in
// tokens of accounts that are SYSTEM, Administrators, or Backup Operators.
// Enabling them is what lets the restore write owner/group/DACL on files the
// restore user does not own — without it, SetSecurityInfo fails with
// ERROR_INVALID_OWNER ("This security ID may not be assigned as the owner of
// this object") and ERROR_ACCESS_DENIED ("Access is denied") on every file
// whose owner/DACL differs from the restore user. This is the standard pattern
// for backup/restore software on Windows.
//
// Note: windows.GetCurrentProcessToken() returns a pseudo-token that grants
// TOKEN_QUERY|DUPLICATE|ASSIGN_PRIMARY but NOT TOKEN_ADJUST_PRIVILEGES, so
// AdjustTokenPrivileges rejects it with ERROR_INVALID_HANDLE ("The handle is
// invalid"). We must OpenProcessToken ourselves with ADJUST_PRIVILEGES access.
//
// Best-effort: if the token genuinely lacks a privilege (e.g. the agent runs
// as a plain user), enabling fails for that privilege and the affected
// metadata operations fail per-file and are reported — but the restore of
// file CONTENT still proceeds for all files.
func prepareRestoreProcess() {
	prepOnce.Do(func() {
		var token windows.Token
		// TOKEN_ADJUST_PRIVILEGES is required to enable privileges; TOKEN_QUERY
		// is required by LookupPrivilegeValue/AdjustTokenPrivileges internals.
		if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token); err != nil {
			syslog.L.Warn().
				WithMessage("restore: could not open process token; metadata errors may follow").
				WithField("error", err.Error()).
				Write()
			return
		}
		defer token.Close()

		var missing []string
		for _, name := range []string{
			"SeRestorePrivilege",       // write owner/group on any object; restore
			"SeBackupPrivilege",        // read any file regardless of ACL
			"SeTakeOwnershipPrivilege", // take ownership of any object
			"SeSecurityPrivilege",      // read/write SACL (auditing)
		} {
			if err := enablePrivilege(token, name); err != nil {
				missing = append(missing, name+" ("+err.Error()+")")
			}
		}
		if len(missing) > 0 {
			syslog.L.Warn().
				WithMessage("restore: some privileges could not be enabled; metadata errors may follow").
				WithField("missing", missing).
				Write()
		}
	})
}

// enablePrivilege enables a single named privilege in the token. Returns an
// error if the privilege is not present in the token (ERROR_NOT_ALL_ASSIGNED)
// or the lookup/adjust failed; the caller treats this as best-effort.
func enablePrivilege(token windows.Token, name string) error {
	n, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, n, &luid); err != nil {
		return err
	}
	tp := windows.Tokenprivileges{
		PrivilegeCount: 1,
		Privileges: [1]windows.LUIDAndAttributes{{
			Luid:       luid,
			Attributes: windows.SE_PRIVILEGE_ENABLED,
		}},
	}
	return windows.AdjustTokenPrivileges(token, false, &tp, 0, nil, nil)
}
