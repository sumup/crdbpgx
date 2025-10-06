package crdbpgx

import (
	"math/rand"
	"time"
)

type Jitter interface {
	// Jitter consumes an internal from a ticker and returns a jittered duration.
	Jitter(time.Duration) time.Duration
}

// Jittered ticker as a time.Ticker with an additional Jitter applied to the duration.
type JitteredTicker struct {
	C         <-chan time.Time
	controlCh chan struct{}
	Jitter
	interval time.Duration
}

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

// Jitter with uniform distribution.
type UniformJitter struct {
	Source *rand.Rand
	Min    time.Duration
}

// Jitter the duration uniformly over the range [Min, d).
// The zero-value is ready to use.  Panics if d <= Min.
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
