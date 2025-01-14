package utils

import "time"

type ExponentialBackoff struct {
	Current    time.Duration
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

func NewExponentialBackoff(initial, max time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		Initial:    initial,
		Max:        max,
		Current:    initial,
		Multiplier: 2.0,
	}
}

func (b *ExponentialBackoff) NextBackOff() time.Duration {
	defer func() {
		b.Current = time.Duration(float64(b.Current) * b.Multiplier)
		if b.Current > b.Max {
			b.Current = b.Max
		}
	}()
	return b.Current
}

func (b *ExponentialBackoff) Reset() {
	b.Current = b.Initial
}
