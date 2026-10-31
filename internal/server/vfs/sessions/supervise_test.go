//go:build linux

package sessions

import (
	"testing"
	"time"
)

func TestSessionWatchExpiresOnlyAfterUninterruptedOutage(t *testing.T) {
	const grace = 2 * time.Minute
	base := time.Unix(1700000000, 0)

	t.Run("connected never expires", func(t *testing.T) {
		var w sessionWatch
		for i := range 100 {
			if w.observe(true, base.Add(time.Duration(i)*time.Minute), grace) {
				t.Fatalf("expired while connected at sample %d", i)
			}
		}
	})

	t.Run("first missing sample starts the timer", func(t *testing.T) {
		var w sessionWatch
		if w.observe(false, base, grace) {
			t.Fatal("expired on the very first missing sample")
		}
	})

	t.Run("outage shorter than grace survives", func(t *testing.T) {
		var w sessionWatch
		w.observe(false, base, grace)
		if w.observe(false, base.Add(grace-time.Second), grace) {
			t.Fatal("expired before the grace period elapsed")
		}
	})

	t.Run("outage reaching grace expires", func(t *testing.T) {
		var w sessionWatch
		w.observe(false, base, grace)
		if !w.observe(false, base.Add(grace), grace) {
			t.Fatal("did not expire after the grace period elapsed")
		}
	})

	t.Run("reconnect resets the timer", func(t *testing.T) {
		var w sessionWatch
		w.observe(false, base, grace)
		w.observe(true, base.Add(time.Minute), grace)

		if w.observe(false, base.Add(2*time.Minute), grace) {
			t.Fatal("expired using the pre-reconnect outage start")
		}
		if w.observe(false, base.Add(2*time.Minute+grace-time.Second), grace) {
			t.Fatal("grace did not restart from the reconnect")
		}
		if !w.observe(false, base.Add(2*time.Minute+grace), grace) {
			t.Fatal("did not expire after the second outage ran past grace")
		}
	})
}
