package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type HandlerFunc func(context.Context, json.RawMessage) (any, error)

func DemoHandlers(flakyProbability float64) map[string]HandlerFunc {
	if flakyProbability < 0 {
		flakyProbability = 0
	}
	if flakyProbability > 1 {
		flakyProbability = 1
	}
	return map[string]HandlerFunc{
		"echo": func(_ context.Context, payload json.RawMessage) (any, error) {
			var value any
			if err := json.Unmarshal(payload, &value); err != nil {
				return nil, fmt.Errorf("decode echo payload: %w", err)
			}
			return value, nil
		},
		"sleep": func(ctx context.Context, payload json.RawMessage) (any, error) {
			var input struct {
				Seconds float64 `json:"seconds"`
			}
			if err := json.Unmarshal(payload, &input); err != nil {
				return nil, fmt.Errorf("decode sleep payload: %w", err)
			}
			if input.Seconds < 0 || input.Seconds > 3600 {
				return nil, errors.New("seconds must be between 0 and 3600")
			}
			timer := time.NewTimer(time.Duration(input.Seconds * float64(time.Second)))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
				return map[string]any{"slept_seconds": input.Seconds}, nil
			}
		},
		"flaky": func(_ context.Context, payload json.RawMessage) (any, error) {
			var value any
			if err := json.Unmarshal(payload, &value); err != nil {
				return nil, fmt.Errorf("decode flaky payload: %w", err)
			}
			if rand.Float64() < flakyProbability {
				return nil, errors.New("simulated flaky handler failure")
			}
			return value, nil
		},
	}
}
