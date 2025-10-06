package crdbpgx

import (
	"context"
	"math"
	"math/rand"
	"time"
)

func exponentialBackoffWithJitter(scalar time.Duration, jitterFraction float64) func(context.Context, uint) time.Duration {
	return func(ctx context.Context, attempt uint) time.Duration {
		// Compute 2^(attempt-1)
		var exp int
		switch {
		case attempt == 0:
			exp = 1
		case attempt >= 62:
			exp = 1 << 62
		default:
			exp = (1 << attempt) >> 1
		}

		dur := time.Duration(int(scalar) * exp)

		// Check for overflow
		if exp != 0 && int(dur/scalar) != exp {
			return time.Duration(math.MaxInt64)
		}

		// add a random jitter to the duration
		//nolint:gosec // weak random is fine here
		factor := jitterFraction * (rand.Float64()*2 - 1)
		return time.Duration(float64(dur) * (1 + factor))
	}
}
