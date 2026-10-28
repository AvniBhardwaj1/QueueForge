package backoff

import (
	"math"
	"time"
)

// Delay computes capped exponential backoff with up to 20% positive jitter.
func Delay(attempt int, base, max time.Duration, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 || max <= 0 {
		return 0
	}
	factor := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(base) * factor)
	if delay > max || delay < 0 {
		delay = max
	}
	if jitter < 0 {
		jitter = 0
	} else if jitter > 1 {
		jitter = 1
	}
	return delay + time.Duration(float64(delay)*0.2*jitter)
}
