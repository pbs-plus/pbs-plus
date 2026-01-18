//go:build linux

package database

import (
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrations embed.FS

func (d *Database) Migrate() error {
	driver, err := sqlite.WithInstance(d.writeDb, &sqlite.Config{})
	if err != nil {
		return err
	}

	fs, err := iofs.New(migrations, "migrations")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("iofs", fs, "sqlite", driver)
	if err != nil {
		return err
	}

	return m.Up()
}
