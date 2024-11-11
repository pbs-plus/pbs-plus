//go:build windows

package utils

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

func ShowMessageBox(title, message string) {
	windows.MessageBox(0,
		windows.StringToUTF16Ptr(message),
		windows.StringToUTF16Ptr(title),
		windows.MB_OK|windows.MB_ICONERROR)
}

func PromptInput(title, prompt string) string {
	cmd := exec.Command("powershell", "-Command", fmt.Sprintf(`
		[void][Reflection.Assembly]::LoadWithPartialName('Microsoft.VisualBasic');
		$input = [Microsoft.VisualBasic.Interaction]::InputBox('%s', '%s');
    $input`, prompt, title))

	// Set SysProcAttr to hide the PowerShell window
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

	output, err := cmd.Output()
	if err != nil {
		fmt.Println("Failed to get input:", err)
		return ""
	}

	return strings.TrimSpace(string(output))
}
