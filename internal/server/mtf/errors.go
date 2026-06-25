package mtf

import "errors"

var (
	ErrChangerRequiresDrive = errors.New("changer scan requires a tape drive device")
	ErrNoSource             = errors.New("no source: provide changer+drive, drive, or bkf path")
	ErrNoBKFFiles           = errors.New("no .bkf files found")
	ErrNoCartridges         = errors.New("no cartridges in media family")
)
