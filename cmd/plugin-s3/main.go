//go:build linux

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/s3"
)

func main() {
	plugin := s3.New()
	err := targetplugin.Serve(context.Background(), s3.Descriptor(), plugin.Handlers())
	plugin.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
