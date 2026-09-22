//go:build linux

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/agentfs"
)

func main() {
	if err := targetplugin.Serve(context.Background(), agentfs.Descriptor(), agentfs.Handlers()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
