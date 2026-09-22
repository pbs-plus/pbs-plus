//go:build linux

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/mysql"
)

func main() {
	if err := targetplugin.Serve(context.Background(), mysql.Descriptor(), mysql.Handlers()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
