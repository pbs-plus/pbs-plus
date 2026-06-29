package changer

import (
	"errors"
	"fmt"
	"time"
)

type Changer struct {
	dev *device
}

func Open(path string) (*Changer, error) {
	d, err := openDevice(path)
	if err != nil {
		return nil, err
	}
	return &Changer{dev: d}, nil
}

func (c *Changer) Close() error { return c.dev.close() }

func (c *Changer) Inventory() error { return initializeElementStatus(c.dev) }

type SlotStatus struct {
	ElementAddress uint16
	Full           bool
	VolumeTag      string // barcode, if Full and readable (else "")
	ImportExport   bool   // mail slot rather than a magazine slot
}

type DriveStatus struct {
	ElementAddress uint16
	Full           bool
	VolumeTag      string // barcode of the loaded cartridge, if known
	LoadedSlotAddr uint16
}

type Status struct {
	TransportAddress uint16
	Slots            []SlotStatus // storage slots, then import/export slots appended
	Drives           []DriveStatus
}

func (c *Changer) Status() (*Status, error) {
	assign, err := readAddressAssignment(c.dev)
	if err != nil {
		return nil, err
	}
	s := &Status{}

	storage, ie, err := c.readStorage(assign)
	if err != nil {
		return nil, err
	}
	s.Slots = append(s.Slots, storage...)
	s.Slots = append(s.Slots, ie...)

	drives, err := c.readElements(assign, elemDrive, assign.FirstTransfer, assign.NumTransfer, true)
	if err != nil {
		return nil, err
	}
	for _, e := range drives {
		s.Drives = append(s.Drives, DriveStatus{
			ElementAddress: e.Addr,
			Full:           e.Full,
			VolumeTag:      e.VolTag,
			LoadedSlotAddr: e.SrcAddr,
		})
	}

	transports, err := c.readElements(assign, elemTransport, assign.FirstTransport, assign.NumTransport, true)
	if err != nil {
		return nil, err
	}
	if len(transports) > 0 {
		s.TransportAddress = transports[0].Addr
	}

	// Sanity-check counts against the address assignment page.
	if len(s.Drives) == 0 {
		return nil, ErrNoDriveElements
	}
	if len(s.Slots) == 0 {
		return nil, ErrNoStorageElements
	}
	return s, nil
}

func (c *Changer) readStorage(assign *addressAssignment) (storage, ie []SlotStatus, err error) {
	st, err := c.readElements(assign, elemStorage, assign.FirstStorage, assign.NumStorage, true)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range st {
		storage = append(storage, SlotStatus{ElementAddress: e.Addr, Full: e.Full, VolumeTag: e.VolTag})
	}
	ieE, err := c.readElements(assign, elemImportExp, assign.FirstImportExp, assign.NumImportExp, true)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range ieE {
		ie = append(ie, SlotStatus{ElementAddress: e.Addr, Full: e.Full, VolumeTag: e.VolTag, ImportExport: true})
	}
	return storage, ie, nil
}

type rawElement struct {
	Addr    uint16
	Full    bool
	ASC     byte
	ASCQ    byte
	SrcAddr uint16
	VolTag  string
}

// readElements queries one element type, paging if the changer caps the
func (c *Changer) readElements(assign *addressAssignment, t elementType, first, count uint16, withVolTag bool) ([]rawElement, error) {
	var out []rawElement
	if count == 0 {
		return out, nil
	}
	start := uint16(0)
	_ = first // first/count from the assignment page bound the expected count only
	const perQuery = 1000
	remaining := count
	for remaining > 0 {
		n := min(uint16(perQuery), remaining)
		elems, last, got, err := c.queryElements(t, start, n, withVolTag)
		if err != nil {
			return nil, fmt.Errorf("read element status type %d @%d: %w", t, start, err)
		}
		out = append(out, elems...)
		if got == 0 || last < start {
			break
		}
		start = last + 1
		remaining -= got
		if uint16(len(elems)) < n {
			break
		}
	}
	return out, nil
}

func (c *Changer) queryElements(t elementType, start, count uint16, withVolTag bool) (elems []rawElement, lastAddr uint16, got uint16, err error) {
	cdb := readElementStatusCDB(start, count, t, withVolTag, allocLenStandard)
	data, err := c.dev.scsi(cdb, make([]byte, allocLenStandard), true, timeoutDefault)
	if err != nil {
		return nil, 0, 0, err
	}
	return decodeElementStatusPage(data, start)
}

func (s *Status) SlotAddress(slot int) (uint16, error) {
	if slot < 1 || slot > len(s.Slots) {
		return 0, fmt.Errorf("slot %d out of range (1..%d)", slot, len(s.Slots))
	}
	return s.Slots[slot-1].ElementAddress, nil
}

func (s *Status) DriveAddress(drive int) (uint16, error) {
	if drive < 0 || drive >= len(s.Drives) {
		return 0, fmt.Errorf("drive %d out of range (0..%d)", drive, len(s.Drives)-1)
	}
	return s.Drives[drive].ElementAddress, nil
}

func (c *Changer) LoadSlot(fromSlot, drivenum int) (*Status, error) {
	status, err := c.Status()
	if err != nil {
		return nil, fmt.Errorf("read status before load: %w", err)
	}
	cdb, err := c.moveCDB(status, fromSlot, drivenum, true)
	if err != nil {
		return nil, err
	}
	if _, err := c.dev.scsi(cdb, nil, false, timeoutMove); err != nil {
		return nil, fmt.Errorf("load slot %d -> drive %d: %w", fromSlot, drivenum, err)
	}
	if err := c.waitForDriveReady(drivenum); err != nil {
		return nil, fmt.Errorf("load slot %d -> drive %d: %w", fromSlot, drivenum, err)
	}
	return c.Status()
}

func (c *Changer) Unload(toSlot, drivenum int) (*Status, error) {
	status, err := c.Status()
	if err != nil {
		return nil, fmt.Errorf("read status before unload: %w", err)
	}
	cdb, err := c.moveCDB(status, toSlot, drivenum, false)
	if err != nil {
		return nil, err
	}
	if _, err := c.dev.scsi(cdb, nil, false, timeoutMove); err != nil {
		return nil, fmt.Errorf("unload drive %d -> slot %d: %w", drivenum, toSlot, err)
	}
	return c.Status()
}

func (c *Changer) Transfer(fromSlot, toSlot int) (*Status, error) {
	status, err := c.Status()
	if err != nil {
		return nil, fmt.Errorf("read status before transfer: %w", err)
	}
	src, err := status.SlotAddress(fromSlot)
	if err != nil {
		return nil, err
	}
	dst, err := status.SlotAddress(toSlot)
	if err != nil {
		return nil, err
	}
	cdb := moveMediumCDB(status.TransportAddress, src, dst)
	if _, err := c.dev.scsi(cdb, nil, false, timeoutMove); err != nil {
		return nil, fmt.Errorf("transfer slot %d -> slot %d: %w", fromSlot, toSlot, err)
	}
	return c.Status()
}

func (c *Changer) moveCDB(status *Status, slot, drivenum int, load bool) ([]byte, error) {
	transport := status.TransportAddress
	slotAddr, err := status.SlotAddress(slot)
	if err != nil {
		return nil, err
	}
	driveAddr, err := status.DriveAddress(drivenum)
	if err != nil {
		return nil, err
	}
	var src, dst uint16
	if load {
		src, dst = slotAddr, driveAddr
	} else {
		src, dst = driveAddr, slotAddr
	}
	return moveMediumCDB(transport, src, dst), nil
}

// ready (Not-Ready/"becoming ready" 04/01 is retried for up to 5 minutes).
func (c *Changer) waitForDriveReady(drive int) error {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		assign, err := readAddressAssignment(c.dev)
		if err != nil {
			return err
		}
		elems, _, _, err := c.queryElements(elemDrive, assign.FirstTransfer+uint16(drive), 1, true)
		if err != nil {
			var se *SenseError
			if errors.As(err, &se) && se.Key == SenseNotReady && se.ASC == 0x04 && se.ASCQ == 0x01 {
				time.Sleep(2 * time.Second)
				continue
			}
			return err
		}
		if len(elems) > 0 && elems[0].Full && elems[0].ASC == 0x00 {
			return nil
		}
		time.Sleep(time.Second)
	}
	return ErrDriveNotReady
}
