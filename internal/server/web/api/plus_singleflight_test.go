//go:build linux

package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchToCacheCoalescesConcurrentRequests(t *testing.T) {
	var hitCount atomic.Int64
	payload := bytes.Repeat([]byte("a"), 4<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount.Add(1)
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	filename := "test-singleflight-coalesce.bin"
	cachePath := filepath.Join(getCacheDir(), filename)
	defer os.Remove(cachePath)

	targetURL := srv.URL + "/" + filename

	const numCallers = 50
	var wg sync.WaitGroup
	errs := make([]error, numCallers)
	start := make(chan struct{})

	wg.Add(numCallers)
	for i := range numCallers {
		go func(idx int) {
			defer wg.Done()
			<-start
			_, err, _ := downloadFlight.Do(filename, func() (any, error) {
				return nil, fetchToCache(targetURL, cachePath)
			})
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d got error: %v", i, err)
		}
	}

	if got := hitCount.Load(); got != 1 {
		t.Errorf("upstream hit %d times, want exactly 1 (coalesced)", got)
	}

	got, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("cached content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestFetchToCacheErrorPropagatesToAllCallers(t *testing.T) {
	var hitCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount.Add(1)
		time.Sleep(100 * time.Millisecond)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	filename := "test-singleflight-error.bin"
	cachePath := filepath.Join(getCacheDir(), filename)
	defer os.Remove(cachePath)

	targetURL := srv.URL + "/" + filename

	const numCallers = 20
	var wg sync.WaitGroup
	errs := make([]error, numCallers)
	start := make(chan struct{})

	wg.Add(numCallers)
	for i := range numCallers {
		go func(idx int) {
			defer wg.Done()
			<-start
			_, err, _ := downloadFlight.Do(filename, func() (any, error) {
				return nil, fetchToCache(targetURL, cachePath)
			})
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	if got := hitCount.Load(); got != 1 {
		t.Errorf("upstream hit %d times, want exactly 1", got)
	}

	for i, err := range errs {
		if err == nil {
			t.Fatalf("caller %d: expected error, got nil", i)
		}
		fe, ok := err.(*fetchError)
		if !ok {
			t.Fatalf("caller %d: expected *fetchError, got %T (%v)", i, err, err)
		}
		if fe.status != http.StatusNotFound {
			t.Errorf("caller %d: status = %d, want %d", i, fe.status, http.StatusNotFound)
		}
	}
}
