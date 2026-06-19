package mtfstore

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrInvalidID      = errors.New("invalid id")
	ErrInvalidMapping = errors.New("invalid namespace mapping")
)

func mapErr(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, what)
	}
	return err
}
