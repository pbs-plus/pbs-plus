package pxarmount

import (
	"encoding/binary"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (j *Journal) pushPendingOne(s pebbleSet) {
	j.pending = append(j.pending, journalOp{s: s})
}

func (j *Journal) pushPendingMany(keys []pebbleSet) {
	j.pending = append(j.pending, journalOp{keys: keys})
}

func (j *Journal) txOne(s pebbleSet) error {
	j.mu.Lock()
	if s.deleteEnd != nil {
		ks, ke := bytesToString(s.key), bytesToString(s.deleteEnd)
		for k := range j.overlay {
			if k >= ks && k < ke {
				delete(j.overlay, k)
			}
		}
	} else if s.delete {
		j.overlay[bytesToString(s.key)] = nil
	} else {
		j.overlay[bytesToString(s.key)] = s.value
	}
	j.pushPendingOne(s)

	drain := len(j.pending) >= 64
	j.mu.Unlock()

	if drain {
		select {
		case j.commitCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (j *Journal) tx(keys ...pebbleSet) error {
	j.mu.Lock()
	for _, s := range keys {
		if s.deleteEnd != nil {
			ks, ke := bytesToString(s.key), bytesToString(s.deleteEnd)
			for k := range j.overlay {
				if k >= ks && k < ke {
					delete(j.overlay, k)
				}
			}
		} else if s.delete {
			j.overlay[bytesToString(s.key)] = nil
		} else {
			j.overlay[bytesToString(s.key)] = s.value
		}
	}
	if len(keys) == 1 {
		j.pushPendingOne(keys[0])
	} else {
		j.pushPendingMany(keys)
	}

	drain := len(j.pending) >= 64
	j.mu.Unlock()

	if drain {
		select {
		case j.commitCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (j *Journal) Sync() error {
	done := make(chan struct{})
	j.mu.Lock()
	j.pushPendingOne(pebbleSet{})
	j.mu.Unlock()

	go func() {
		for {
			j.mu.Lock()
			hasPending := len(j.pending) > 0
			j.mu.Unlock()
			if !hasPending {
				close(done)
				return
			}
			select {
			case j.commitCh <- struct{}{}:
			default:
			}
			time.Sleep(time.Millisecond)
		}
	}()

	select {
	case j.commitCh <- struct{}{}:
	default:
	}

	<-done

	j.mu.Lock()
	err := j.commitErr
	j.commitErr = nil
	j.mu.Unlock()
	if err != nil {
		return err
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	return j.persistNextNodeID()
}

func (j *Journal) commitLoop() {
	defer close(j.stopped)
	ticker := time.NewTicker(commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-j.stopCh:
			j.drainAllLocked()
			return
		case <-ticker.C:
			j.drainAllLocked()
		case <-j.commitCh:
			j.drainAllLocked()
		}
	}
}

func (j *Journal) drainAllLocked() {
	j.mu.Lock()
	if len(j.pending) == 0 {
		j.mu.Unlock()
		return
	}
	pending := j.pending
	j.pending = j.pending[:0]
	j.overlay = make(map[string][]byte)

	pb := j.db.NewBatch()
	for _, op := range pending {
		if len(op.keys) > 0 {
			for _, s := range op.keys {
				if s.deleteEnd != nil {
					if err := pb.DeleteRange(s.key, s.deleteEnd, nil); err != nil {
						log.Error(err, "")
					}
				} else if s.delete {
					if err := pb.Delete(s.key, nil); err != nil {
						log.Error(err, "")
					}
				} else {
					if err := pb.Set(s.key, s.value, nil); err != nil {
						log.Error(err, "")
					}
				}
			}
		} else {
			s := op.s
			if s.deleteEnd != nil {
				if err := pb.DeleteRange(s.key, s.deleteEnd, nil); err != nil {
					log.Error(err, "")
				}
			} else if s.delete {
				if err := pb.Delete(s.key, nil); err != nil {
					log.Error(err, "")
				}
			} else {
				if err := pb.Set(s.key, s.value, nil); err != nil {
					log.Error(err, "")
				}
				if len(s.key) >= 2 && s.key[0] == 'n' && s.key[1] == ':' {
					nid := int64(binary.BigEndian.Uint64(s.key[2:]))
					if err := pb.Set(checksumKey(nid), encodeUint32(fnv32(s.value)), nil); err != nil {
						log.Error(err, "")
					}
				}
			}
		}
	}

	err := pb.Commit(pebble.Sync)
	if err := pb.Close(); err != nil {
		log.Error(err, "")
	}
	if err != nil {
		j.commitErr = err
	}
	j.mu.Unlock()
}
