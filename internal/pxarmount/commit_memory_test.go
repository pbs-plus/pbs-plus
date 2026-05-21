package pxarmount

import (
	"fmt"
	"runtime"
	"testing"
)

// TestCommitWalkMemoryRetention proves that pxarEntries survives ALL
// recursive subdirectory processing in the current commitWalk design.
//
// The test uses runtime finalizers on the backing array.  In the
// nested style (current code), the backing array is reachable DURING
// the inner loop because we access entries by index — the same pattern
// as the two-pointer merge in commitWalk.  In the deferred style
// (planned fix), we extract dir metadata, nil the slice, force GC,
// and verify the old backing array was collected before recursing.
func TestCommitWalkMemoryRetention(t *testing.T) {
	const N = 100 // root entries

	t.Run("nested-entries-alive-during-recursion", func(t *testing.T) {
		runtime.GC()

		entries := make([]dirEntrySlim, N)
		for i := range entries {
			entries[i].name = fmt.Sprintf("entry_%04d", i)
		}

		backingPtr := &entries[0]
		freed := make(chan struct{}, 1)
		runtime.SetFinalizer(backingPtr, func(_ *dirEntrySlim) {
			select {
			case freed <- struct{}{}:
			default:
			}
		})

		// Simulate commitWalk's two-pointer merge loop where directory
		// recursion happens INSIDE the loop body.  We check the finalizer
		// state at each iteration while entries[pi] is still live.
		pi := 0
		for pi < len(entries) {
			// Access entries[pi] — keeps the backing array on the stack.
			// Using a non-dead-store pattern so the compiler can't elide it.
			if entries[pi].name == "" {
				panic("unreachable")
			}

			// Allocate sub-entries (like recursive readDirRaw).
			_ = makeDirEntries(20)

			// Explicit GC to check if entries was collected mid-loop.
			runtime.GC()
			select {
			case <-freed:
				t.Fatalf("entries backing array GC'd at iteration %d: OOM bug does NOT exist", pi)
			default:
				// Expected: entries still alive during iteration.
			}

			pi++
		}

		t.Log("PASS: entries survived all iterations — OOM bug confirmed")
	})

	t.Run("deferred-entries-freed-before-recursion", func(t *testing.T) {
		runtime.GC()

		entries := make([]dirEntrySlim, N)
		for i := range entries {
			entries[i].name = fmt.Sprintf("entry_%04d", i)
		}

		backingPtr := &entries[0]
		freed := make(chan struct{}, 1)
		runtime.SetFinalizer(backingPtr, func(_ *dirEntrySlim) {
			select {
			case freed <- struct{}{}:
			default:
			}
		})

		// Extract directory metadata (the fix).
		type dirInfo struct{ name string }
		dirs := make([]dirInfo, len(entries))
		for i := range entries {
			dirs[i] = dirInfo{entries[i].name}
		}

		// Drop the large slice.
		entries = nil
		_ = entries
		runtime.GC()

		// Wait for GC to run the finalizer.
		for range 20 {
			runtime.GC()
			select {
			case <-freed:
				t.Log("entries freed BEFORE recursion — OOM fix works")
				goto freedOk
			default:
			}
		}
		t.Fatal("entries not freed despite nil + GC — fix may not be effective")
	freedOk:

		// Now recurse — old entries are gone, only small dirs struct alive.
		for range dirs {
			_ = makeDirEntries(20)
		}
	})
}

// TestCommitWalkMemoryRetentionDeep models a 3-level deep tree to show
// that in the nested pattern, entries from ALL ancestor levels stay
// alive simultaneously, while the deferred pattern only holds one
// level at a time.
func TestCommitWalkMemoryRetentionDeep(t *testing.T) {
	const (
		level1Dirs  = 50
		level2Dirs  = 30
		level3Files = 10
	)

	t.Run("nested-all-levels-alive", func(t *testing.T) {
		runtime.GC()

		// Level 1 entries.
		l1 := make([]dirEntrySlim, level1Dirs)
		for i := range l1 {
			l1[i].name = fmt.Sprintf("l1_%04d", i)
		}
		l1Ptr := &l1[0]
		l1Freed := make(chan struct{}, 1)
		runtime.SetFinalizer(l1Ptr, func(_ *dirEntrySlim) {
			select {
			case l1Freed <- struct{}{}:
			default:
			}
		})

		// Iterate level 1 (like commitWalk's merge loop).
		for i := range l1 {
			if l1[i].name == "" {
				panic("unreachable")
			}

			// Level 2 entries allocated while level 1 is alive.
			l2 := make([]dirEntrySlim, level2Dirs)
			for j := range l2 {
				l2[j].name = fmt.Sprintf("l2_%04d_%04d", i, j)
			}
			l2Ptr := &l2[0]
			l2Freed := make(chan struct{}, 1)
			runtime.SetFinalizer(l2Ptr, func(_ *dirEntrySlim) {
				select {
				case l2Freed <- struct{}{}:
				default:
				}
			})

			// Iterate level 2 while both l1 and l2 are alive.
			for j := range l2 {
				if l2[j].name == "" {
					panic("unreachable")
				}
				_ = makeDirEntries(level3Files)
			}

			// Check: l1 should still be alive after processing l2.
			runtime.GC()
			select {
			case <-l1Freed:
				t.Fatal("l1 entries GC'd while still iterating l1 — compiler elided")
			default:
			}

			// l2 goes out of scope here.
			_ = l2
		}

		t.Log("PASS: all ancestor levels alive during nested recursion")
	})

	t.Run("deferred-only-current-level-alive", func(t *testing.T) {
		runtime.GC()

		l1 := make([]dirEntrySlim, level1Dirs)
		for i := range l1 {
			l1[i].name = fmt.Sprintf("l1_%04d", i)
		}
		l1Ptr := &l1[0]
		l1Freed := make(chan struct{}, 1)
		runtime.SetFinalizer(l1Ptr, func(_ *dirEntrySlim) {
			select {
			case l1Freed <- struct{}{}:
			default:
			}
		})

		// Extract dir metadata from l1.
		type dirInfo struct{ name string }
		l1Dirs := make([]dirInfo, len(l1))
		for i := range l1 {
			l1Dirs[i] = dirInfo{l1[i].name}
		}

		// Free l1 before recursing.
		l1 = nil
		_ = l1
		runtime.GC()

		// Verify l1 is freed.
		for range 20 {
			runtime.GC()
			select {
			case <-l1Freed:
				goto l1FreedOk
			default:
			}
		}
		t.Fatal("l1 not freed despite nil + GC")
	l1FreedOk:
		t.Log("l1 freed before processing any subdirectories")

		// Now process l2 directories — l1 is gone.
		for range l1Dirs {
			l2 := make([]dirEntrySlim, level2Dirs)
			for j := range l2 {
				l2[j].name = "x"
			}
			l2Dirs := make([]dirInfo, len(l2))
			for j := range l2 {
				l2Dirs[j] = dirInfo{l2[j].name}
			}
			l2 = nil
			_ = l2
			_ = l2Dirs
			// Process level 3...
			for range l2Dirs {
				_ = makeDirEntries(level3Files)
			}
		}
	})
}
