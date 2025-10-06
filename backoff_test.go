package crdbpgx

import (
	"testing"
	"time"
)

func TestExponentialBackoffWithJitter(t *testing.T) {
	scalar := 100 * time.Millisecond
	jitterFrac := 0.10
	backoffFunc := exponentialBackoffWithJitter(scalar, jitterFrac)
	// use 64 so we are past number of attempts where exponentBase2 would overflow
	for i := 0; i < 64; i++ {
		waitFor := backoffFunc(nil, uint(i))
		if waitFor < 0 {
			t.Errorf("exponentialBackoffWithJitter(%d) = %d; want >= 0", i, waitFor)
		}
	}
}
