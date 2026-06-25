package log

import (
	"errors"
	"log/slog"
	"testing"
)

var errTest = errors.New("test error")

func BenchmarkInfo(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Info("test message")
	}
}

func BenchmarkInfoWithFields(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Info("test message", "key1", "value1", "key2", 42)
	}
}

func BenchmarkError(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Error(errTest, "test error")
	}
}

func BenchmarkErrorWithFields(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Error(errTest, "test error", "key", "value", "code", 500)
	}
}

func BenchmarkWarn(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Warn("test warning")
	}
}

func BenchmarkDebug(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Debug("test debug")
	}
}

func BenchmarkDebugWithFields(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Debug("test debug", "key", "value", "num", 123)
	}
}

func BenchmarkWithScope(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		WithScope(Scope{JobID: "test-job"})
	}
}

func BenchmarkScopedInfo(b *testing.B) {
	logger := WithScope(Scope{JobID: "test-job"})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("scoped message")
	}
}

func BenchmarkScopedDebug(b *testing.B) {
	logger := WithScope(Scope{JobID: "test-job"})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Debug("scoped debug")
	}
}

func BenchmarkDedupCheck(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Info("unique message " + string(rune('a'+b.N%26)))
	}
}

func TestAllocSanity(t *testing.T) {
	if L.enabled(slog.LevelDebug) {
		t.Skip("debug level is enabled, skipping zero-alloc test")
	}
	r := testing.AllocsPerRun(100, func() {
		Debug("should not allocate")
	})
	if r > 0 {
		t.Errorf("Debug at info level should not allocate, got %v allocs", r)
	}
}
