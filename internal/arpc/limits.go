package arpc

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/elastic/go-sysinfo"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/quic-go/quic-go"
)

func parseSize(s string, def int64) int64 {
	if s == "" {
		return def
	}
	s = strings.TrimSpace(s)
	allDigits := true
	for _, r := range s {
		if !unicode.IsDigit(r) && r != '.' {
			allDigits = false
			break
		}
	}
	if allDigits {
		if strings.ContainsRune(s, '.') {
			f, err := strconv.ParseFloat(s, 64)
			if err == nil && f > 0 {
				return int64(f)
			}
			return def
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err == nil && v > 0 {
			return v
		}
		return def
	}
	str := strings.ToLower(s)
	mult := int64(1)
	switch {
	case strings.HasSuffix(str, "kib"):
		mult = 1024
		str = strings.TrimSuffix(str, "kib")
	case strings.HasSuffix(str, "mib"):
		mult = 1024 * 1024
		str = strings.TrimSuffix(str, "mib")
	case strings.HasSuffix(str, "gib"):
		mult = 1024 * 1024 * 1024
		str = strings.TrimSuffix(str, "gib")
	case strings.HasSuffix(str, "kb"):
		mult = 1000
		str = strings.TrimSuffix(str, "kb")
	case strings.HasSuffix(str, "mb"):
		mult = 1000 * 1000
		str = strings.TrimSuffix(str, "mb")
	case strings.HasSuffix(str, "gb"):
		mult = 1000 * 1000 * 1000
		str = strings.TrimSuffix(str, "gb")
	case strings.HasSuffix(str, "k"):
		mult = 1000
		str = strings.TrimSuffix(str, "k")
	case strings.HasSuffix(str, "m"):
		mult = 1000 * 1000
		str = strings.TrimSuffix(str, "m")
	case strings.HasSuffix(str, "g"):
		mult = 1000 * 1000 * 1000
		str = strings.TrimSuffix(str, "g")
	case strings.HasSuffix(str, "ki"):
		mult = 1024
		str = strings.TrimSuffix(str, "ki")
	case strings.HasSuffix(str, "mi"):
		mult = 1024 * 1024
		str = strings.TrimSuffix(str, "mi")
	case strings.HasSuffix(str, "gi"):
		mult = 1024 * 1024 * 1024
		str = strings.TrimSuffix(str, "gi")
	case strings.HasSuffix(str, "b"):
		mult = 1
		str = strings.TrimSuffix(str, "b")
	}
	num := strings.TrimSpace(str)
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return def
	}
	res := int64(f * float64(mult))
	if res <= 0 {
		return def
	}
	return res
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return def
}

func clamp64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

const (
	qgDefInitStream = 512 << 10 // 512 KiB
	qgDefInitConn   = 512 << 10 // 512 KiB
	qgDefMaxStream  = 6 << 20   // 6 MiB
	qgDefMaxConn    = 15 << 20  // 15 MiB
)

func quicServerLimitsAutoConfig() *quic.Config {
	initStream := parseSize(os.Getenv("PBS_PLUS_ARPC_INIT_STREAM_WIN"), 0)
	initConn := parseSize(os.Getenv("PBS_PLUS_ARPC_INIT_CONN_WIN"), 0)
	maxStream := parseSize(os.Getenv("PBS_PLUS_ARPC_MAX_STREAM_WIN"), 0)
	maxConn := parseSize(os.Getenv("PBS_PLUS_ARPC_MAX_CONN_WIN"), 0)

	var memTotal uint64
	if host, err := sysinfo.Host(); err == nil {
		if mem, err := host.Memory(); err == nil {
			memTotal = mem.Total
		}
	}

	N := envInt("PBS_PLUS_ARPC_CONN_TARGET", 100)
	if N <= 0 {
		N = 100
	}
	ramFrac := envFloat("PBS_PLUS_ARPC_RAM_FRACTION", 0.10)
	if ramFrac <= 0 {
		ramFrac = 0.10
	}
	if ramFrac > 0.50 {
		ramFrac = 0.50
	}

	minConn := parseSize(os.Getenv("PBS_PLUS_ARPC_MIN_CONN_WIN"), qgDefMaxConn)
	minStream := parseSize(os.Getenv("PBS_PLUS_ARPC_MIN_STREAM_WIN"), qgDefMaxStream)
	absMaxConn := parseSize(os.Getenv("PBS_PLUS_ARPC_ABS_MAX_CONN_WIN"), 256<<20)
	absMaxStream := parseSize(os.Getenv("PBS_PLUS_ARPC_ABS_MAX_STREAM_WIN"), 64<<20)

	// Compute targets if we have mem info
	var perConnCap, perStreamCap int64
	if memTotal > 0 {
		totalBudget := int64(float64(memTotal) * ramFrac)
		perConnCap = totalBudget / int64(N)
		perConnCap = clamp64(perConnCap, minConn, absMaxConn)

		perStreamCap = perConnCap / 4
		perStreamCap = clamp64(perStreamCap, minStream, absMaxStream)
		if perStreamCap > perConnCap/2 {
			perStreamCap = perConnCap / 2
		}
	}

	// Fill maxima: env takes precedence, else RAM-based, else quic-go defaults
	if maxConn == 0 {
		if perConnCap > 0 {
			maxConn = perConnCap
		} else {
			maxConn = qgDefMaxConn
		}
	}
	if maxStream == 0 {
		if perStreamCap > 0 {
			maxStream = perStreamCap
		} else {
			maxStream = qgDefMaxStream
		}
	}

	// Initials: env precedence, else derived from maxima with floors
	if initStream == 0 {
		initStream = maxStream / 4
		if initStream > 2<<20 {
			initStream = 2 << 20
		}
		if initStream < qgDefInitStream {
			initStream = qgDefInitStream
		}
	}
	if initConn == 0 {
		initConn = maxConn / 4
		if initConn > 4<<20 {
			initConn = 4 << 20
		}
		if initConn < qgDefInitConn {
			initConn = qgDefInitConn
		}
	}

	// Relationships and clamps
	if maxStream < initStream {
		maxStream = initStream
	}
	if maxConn < initConn {
		maxConn = initConn
	}
	if maxConn < maxStream*2 {
		maxConn = maxStream * 2
	}
	maxConn = clamp64(maxConn, minConn, absMaxConn)
	maxStream = clamp64(maxStream, minStream, absMaxStream)
	if initStream > maxStream {
		initStream = maxStream
	}
	if initConn > maxConn {
		initConn = maxConn
	}

	cfg := &quic.Config{
		InitialStreamReceiveWindow:     uint64(initStream),
		InitialConnectionReceiveWindow: uint64(initConn),
		MaxStreamReceiveWindow:         uint64(maxStream),
		MaxConnectionReceiveWindow:     uint64(maxConn),
	}

	if memTotal > 0 {
		syslog.L.Debug().
			WithField("totalRAM", fmt.Sprintf("%dB", memTotal)).
			WithField("budget", fmt.Sprintf("%.2f%%", ramFrac*100)).
			WithField("N", N).
			WithField("initStreamReceiveWindow", cfg.InitialStreamReceiveWindow).
			WithField("initConnReceiveWindow", cfg.InitialConnectionReceiveWindow).
			WithField("maxStreamReceiveWindow", cfg.MaxStreamReceiveWindow).
			WithField("maxConnReceiveWindow", cfg.MaxConnectionReceiveWindow).
			WithMessage("aRPC QUIC limits auto configured").
			Write()
	} else {
		syslog.L.Debug().
			WithField("initStreamReceiveWindow", cfg.InitialStreamReceiveWindow).
			WithField("initConnReceiveWindow", cfg.InitialConnectionReceiveWindow).
			WithField("maxStreamReceiveWindow", cfg.MaxStreamReceiveWindow).
			WithField("maxConnReceiveWindow", cfg.MaxConnectionReceiveWindow).
			WithMessage("aRPC QUIC limits configured (no RAM info; env/defaults used)").
			Write()
	}

	return cfg
}
