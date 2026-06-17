package mtfstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/mtfstore/mtfquery"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrInvalidID      = errors.New("invalid id")
	ErrInvalidMapping = errors.New("invalid namespace mapping")
)

// --- Changers ---

func (d *Database) CreateChanger(ctx context.Context, c Changer) error {
	return d.queries.CreateChanger(ctx, mtfquery.CreateChangerParams{
		Name:    c.Name,
		Device:  c.Device,
		Comment: sql.NullString{String: c.Comment, Valid: c.Comment != ""},
	})
}

func (d *Database) GetChanger(ctx context.Context, name string) (Changer, error) {
	c, err := d.readQueries.GetChanger(ctx, name)
	if err != nil {
		return Changer{}, mapErr(err, "changer")
	}
	return changerFromRow(c), nil
}

func (d *Database) ListChangers(ctx context.Context) ([]Changer, error) {
	rows, err := d.readQueries.ListChangers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Changer, 0, len(rows))
	for _, r := range rows {
		out = append(out, changerFromRow(r))
	}
	return out, nil
}

func (d *Database) UpdateChanger(ctx context.Context, c Changer) error {
	return d.queries.UpdateChanger(ctx, mtfquery.UpdateChangerParams{
		Device:  c.Device,
		Comment: sql.NullString{String: c.Comment, Valid: c.Comment != ""},
		Name:    c.Name,
	})
}

func (d *Database) DeleteChanger(ctx context.Context, name string) error {
	_, err := d.queries.DeleteChanger(ctx, name)
	return err
}

func changerFromRow(r mtfquery.MtfChanger) Changer {
	return Changer{
		Name:      r.Name,
		Device:    r.Device,
		Comment:   ns(r.Comment),
		CreatedAt: ni64(r.CreatedAt),
	}
}

// --- Drives ---

func (d *Database) CreateDrive(ctx context.Context, dr Drive) error {
	return d.queries.CreateDrive(ctx, mtfquery.CreateDriveParams{
		Name:       dr.Name,
		Device:     dr.Device,
		Changer:    sql.NullString{String: dr.Changer, Valid: dr.Changer != ""},
		DriveIndex: sql.NullInt64{Int64: int64(dr.DriveIndex), Valid: true},
		Comment:    sql.NullString{String: dr.Comment, Valid: dr.Comment != ""},
	})
}

func (d *Database) GetDrive(ctx context.Context, name string) (Drive, error) {
	r, err := d.readQueries.GetDrive(ctx, name)
	if err != nil {
		return Drive{}, mapErr(err, "drive")
	}
	return driveFromRow(r), nil
}

func (d *Database) ListDrives(ctx context.Context) ([]Drive, error) {
	rows, err := d.readQueries.ListDrives(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Drive, 0, len(rows))
	for _, r := range rows {
		out = append(out, driveFromRow(r))
	}
	return out, nil
}

func (d *Database) UpdateDrive(ctx context.Context, dr Drive) error {
	return d.queries.UpdateDrive(ctx, mtfquery.UpdateDriveParams{
		Device:     dr.Device,
		Changer:    sql.NullString{String: dr.Changer, Valid: dr.Changer != ""},
		DriveIndex: sql.NullInt64{Int64: int64(dr.DriveIndex), Valid: true},
		Comment:    sql.NullString{String: dr.Comment, Valid: dr.Comment != ""},
		Name:       dr.Name,
	})
}

func (d *Database) DeleteDrive(ctx context.Context, name string) error {
	_, err := d.queries.DeleteDrive(ctx, name)
	return err
}

func driveFromRow(r mtfquery.MtfDrife) Drive {
	return Drive{
		Name:       r.Name,
		Device:     r.Device,
		Changer:    ns(r.Changer),
		DriveIndex: ni(r.DriveIndex),
		Comment:    ns(r.Comment),
		CreatedAt:  ni64(r.CreatedAt),
	}
}

func mapErr(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, what)
	}
	return err
}
