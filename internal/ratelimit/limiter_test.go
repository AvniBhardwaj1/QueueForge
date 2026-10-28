package ratelimit

import (
	"testing"
	"time"
)

func TestLimiterRefillsAndCleans(t *testing.T) {
	now := time.Unix(100, 0)
	l := NewWithClock(2, 2, time.Minute, func() time.Time { return now })
	if ok, _ := l.Allow("client"); !ok {
		t.Fatal("first request denied")
	}
	if ok, _ := l.Allow("client"); !ok {
		t.Fatal("second request denied")
	}
	ok, retry := l.Allow("client")
	if ok || retry != 500*time.Millisecond {
		t.Fatalf("third request = %v, %s", ok, retry)
	}
	now = now.Add(500 * time.Millisecond)
	if ok, _ := l.Allow("client"); !ok {
		t.Fatal("request did not refill")
	}
	now = now.Add(2 * time.Minute)
	l.Cleanup()
	if len(l.buckets) != 0 {
		t.Fatal("idle bucket was not removed")
	}
}
