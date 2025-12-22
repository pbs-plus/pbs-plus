package utils

import (
	"fmt"
	"os/exec"
)

func RunSystemd(serviceName, description, binary string, args []string) error {
	cmdArgs := []string{
		"--unit=" + serviceName,
		"--description=" + description,
		"--remain-after-exit",
		"--collect",
		"--property=Type=simple",
		"--property=KillMode=control-group",
		"--property=Restart=no",
		binary,
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("systemd-run", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemd-run failed: %w, output: %s", err, string(output))
	}

	return nil
}
