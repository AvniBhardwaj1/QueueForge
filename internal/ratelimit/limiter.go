package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
	seen   time.Time
}

type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
	idleTTL time.Duration
	now     func() time.Time
}

func New(rate float64, burst int, idleTTL time.Duration) *Limiter {
	return NewWithClock(rate, burst, idleTTL, time.Now)
}

func NewWithClock(rate float64, burst int, idleTTL time.Duration, now func() time.Time) *Limiter {
	if rate <= 0 {
		rate = 20
	}
	if burst < 1 {
		burst = 40
	}
	if idleTTL <= 0 {
		idleTTL = 10 * time.Minute
	}
	return &Limiter{buckets: make(map[string]*bucket), rate: rate, burst: float64(burst), idleTTL: idleTTL, now: now}
}

func (l *Limiter) Allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	b.seen = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}

func (l *Limiter) Cleanup() {
	cutoff := l.now().Add(-l.idleTTL)
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, b := range l.buckets {
		if b.seen.Before(cutoff) {
			delete(l.buckets, key)
		}
	}
}
