package changer

import "errors"

var (
	ErrElementStatusTooShort  = errors.New("element-status response too short")
	ErrElementDescZero        = errors.New("element descriptor length is zero")
	ErrElementAddressNotFound = errors.New("mode sense 0x1D: element-address page not found")
	ErrNoDriveElements        = errors.New("changer reported no data-transfer (drive) elements")
	ErrNoStorageElements      = errors.New("changer reported no storage elements")
	ErrDriveNotReady          = errors.New("timed out waiting for drive to become ready")
)
