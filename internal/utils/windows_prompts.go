//go:build windows

package utils

import (
	"golang.org/x/sys/windows"
)

func ShowMessageBox(title, message string) {
	windows.MessageBox(0,
		windows.StringToUTF16Ptr(message),
		windows.StringToUTF16Ptr(title),
		windows.MB_OK|windows.MB_ICONERROR)
}
