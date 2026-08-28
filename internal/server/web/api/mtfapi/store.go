//go:build linux

package mtfapi

import (
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

func mtfStore(app *application.Runtime) *mtfdb.Store {
	if app == nil {
		return nil
	}
	return app.MtfDB
}
