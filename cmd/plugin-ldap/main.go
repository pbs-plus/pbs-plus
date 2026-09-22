//go:build linux

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/ldap"
)

func main() {
	if err := targetplugin.Serve(context.Background(), ldap.Descriptor(), ldap.Handlers()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
