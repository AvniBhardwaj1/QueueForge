package backoff

import (
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"first attempt", 1, 2 * time.Second},
		{"second attempt", 2, 4 * time.Second},
		{"capped", 20, 5 * time.Minute},
		{"nonpositive attempts", 0, 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Delay(tt.attempt, 2*time.Second, 5*time.Minute, 0)
			if got != tt.want {
				t.Fatalf("Delay() = %s, want %s", got, tt.want)
			}
		})
	}
	if got := Delay(1, time.Second, time.Minute, 1); got != 1200*time.Millisecond {
		t.Fatalf("jittered delay = %s, want 1.2s", got)
	}
}
