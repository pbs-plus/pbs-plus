//go:build windows

package pxar

import (
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

var prepOnce sync.Once

// prepareRestoreProcess enables backup/restore/owner privileges in the process
// token so metadata (owner, group, DACL) can be written on files the restore user
// windows.GetCurrentProcessToken() returns a pseudo-token that grants
// TOKEN_QUERY|DUPLICATE|ASSIGN_PRIMARY but NOT TOKEN_ADJUST_PRIVILEGES, so we
// must OpenProcessToken ourselves. If the token lacks a privilege, that
func prepareRestoreProcess() {
	prepOnce.Do(func() {
		var token windows.Token
		// TOKEN_ADJUST_PRIVILEGES is required to enable privileges.
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
			"SeRestorePrivilege",
			"SeBackupPrivilege",
			"SeTakeOwnershipPrivilege",
			"SeSecurityPrivilege",
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
// error if the privilege is not present or the lookup/adjust failed.
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
