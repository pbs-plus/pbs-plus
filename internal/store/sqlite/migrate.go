//go:build linux

package sqlite

import (
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrations embed.FS

func (d *Database) Migrate() error {
	fs, err := iofs.New(migrations, "migrations")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithSourceInstance("iofs", fs, "sqlite://"+d.dbPath)
	if err != nil {
		return err
	}

	return m.Up()
}
