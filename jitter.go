package crdbpgx

import (
	"math/rand"
	"time"
)

// Jitter applies randomness to durations to prevent thundering herd problems.
type Jitter interface {
	// Jitter consumes an interval from a ticker and returns a jittered duration.
	Jitter(time.Duration) time.Duration
}

// JitteredTicker is a ticker with an additional jitter applied to the interval.
// Similar to time.Ticker but with randomized intervals.
type JitteredTicker struct {
	// Jitter determines the delay between ticks. It must not be nil.
	Jitter
	// C receives ticks and is closed when the ticker's loop stops.
	C         <-chan time.Time
	controlCh chan struct{}
	interval  time.Duration
}

// NewJitteredTicker creates a new jittered ticker that ticks at intervals
// around the specified duration, modified by the provided jitter.
func NewJitteredTicker(d time.Duration, j Jitter) *JitteredTicker {
	c := make(chan time.Time)
	t := &JitteredTicker{
		C:         c,
		controlCh: make(chan struct{}),
		Jitter:    j,
		interval:  d,
	}
	go t.loop(c)
	return t
}

// Stop requests that the ticker stop and close C. It must be called only once.
func (t *JitteredTicker) Stop() {
	close(t.controlCh)
}

func (t *JitteredTicker) loop(c chan<- time.Time) {
	defer close(c)

	for {
		nextDelay := t.Jitter.Jitter(t.interval)
		select {
		case <-t.controlCh:
			return
		case <-time.After(nextDelay):
		}

		select {
		case <-t.controlCh:
			return
		case c <- time.Now():
		default:
		}
	}
}

// UniformJitter implements uniform jitter distribution.
//
// The jittered duration is uniformly distributed between Min and the provided duration.
// This helps distribute load more evenly and prevents synchronized retries across clients.
type UniformJitter struct {
	// Source supplies randomness. A nil Source uses the default random source.
	// A non-nil Source must not be shared concurrently without synchronization.
	Source *rand.Rand
	// Min is the inclusive lower bound of the jittered duration.
	Min time.Duration
}

// Jitter returns a duration uniformly distributed over the range [Min, d).
// The zero-value is ready to use with the default random source.
// Panics if d <= Min.
func (u UniformJitter) Jitter(d time.Duration) time.Duration {
	drawUniform := rand.Int63n
	if u.Source != nil {
		drawUniform = u.Source.Int63n
	}

	delta := d - u.Min
	if delta <= 0 {
		panic("duration must exceed min")
	}
	return u.Min + time.Duration(drawUniform(int64(delta)))
}
