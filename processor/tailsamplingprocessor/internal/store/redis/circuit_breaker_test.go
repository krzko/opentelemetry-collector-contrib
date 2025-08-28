// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreakerConfig_SetDefaults(t *testing.T) {
	config := CircuitBreakerConfig{}
	config.SetDefaults()
	
	assert.Equal(t, 5, config.FailureThreshold)
	assert.Equal(t, 3, config.SuccessThreshold)
	assert.Equal(t, 30*time.Second, config.OpenDuration)
	assert.Equal(t, 3, config.HalfOpenMaxAttempts)
	assert.Equal(t, 1*time.Minute, config.ObservationWindow)
}

func TestCircuitBreakerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  CircuitBreakerConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   1 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "invalid failure threshold",
			config: CircuitBreakerConfig{
				FailureThreshold:    0,
				SuccessThreshold:    3,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "failure_threshold must be at least 1",
		},
		{
			name: "invalid success threshold",
			config: CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    0,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "success_threshold must be at least 1",
		},
		{
			name: "invalid open duration",
			config: CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				OpenDuration:        0,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "open_duration must be positive",
		},
		{
			name: "invalid half open max attempts",
			config: CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 0,
				ObservationWindow:   1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "half_open_max_attempts must be at least 1",
		},
		{
			name: "invalid observation window",
			config: CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   0,
			},
			wantErr: true,
			errMsg:  "observation_window must be positive",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewCircuitBreaker(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := CircuitBreakerConfig{
			FailureThreshold:    5,
			SuccessThreshold:    3,
			OpenDuration:        30 * time.Second,
			HalfOpenMaxAttempts: 3,
			ObservationWindow:   1 * time.Minute,
		}
		
		cb, err := NewCircuitBreaker(config)
		require.NoError(t, err)
		assert.NotNil(t, cb)
		assert.Equal(t, StateClosed, cb.GetState())
	})
	
	t.Run("invalid config", func(t *testing.T) {
		config := CircuitBreakerConfig{
			FailureThreshold:    0, // Invalid
			SuccessThreshold:    3, // Valid
			OpenDuration:        30 * time.Second, // Valid
			HalfOpenMaxAttempts: 3, // Valid
			ObservationWindow:   1 * time.Minute, // Valid
		}
		
		cb, err := NewCircuitBreaker(config)
		require.Error(t, err)
		assert.Nil(t, cb)
		assert.Contains(t, err.Error(), "failure_threshold must be at least 1")
	})
}

func TestCircuitBreaker_BasicFlow(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	
	// Initial state should be closed
	assert.Equal(t, StateClosed, cb.GetState())
	
	// Successful calls should work
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateClosed, cb.GetState())
	
	// Trigger failures to open circuit
	testErr := errors.New("test error")
	for i := 0; i < 3; i++ {
		err = cb.Call(ctx, func() error {
			return testErr
		})
		assert.Equal(t, testErr, err)
	}
	
	// Circuit should be open
	assert.Equal(t, StateOpen, cb.GetState())
	
	// Calls should fail immediately with ErrCircuitOpen
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.Equal(t, ErrCircuitOpen, err)
	
	// Wait for open duration to pass
	time.Sleep(150 * time.Millisecond)
	
	// Circuit should transition to half-open on next attempt
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateHalfOpen, cb.GetState())
	
	// One more success should close the circuit
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateClosed, cb.GetState())
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 3,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	assert.Equal(t, StateOpen, cb.GetState())
	
	// Wait for half-open
	time.Sleep(150 * time.Millisecond)
	
	// First call transitions to half-open and succeeds
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateHalfOpen, cb.GetState())
	
	// Failure in half-open should reopen circuit
	err = cb.Call(ctx, func() error {
		return testErr
	})
	assert.Equal(t, testErr, err)
	assert.Equal(t, StateOpen, cb.GetState())
}

func TestCircuitBreaker_HalfOpenMaxAttempts(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	
	// Wait for half-open
	time.Sleep(150 * time.Millisecond)
	
	// Use up half-open attempts
	var wg sync.WaitGroup
	var attempts atomic.Int32
	
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cb.Call(ctx, func() error {
				attempts.Add(1)
				time.Sleep(50 * time.Millisecond) // Simulate work
				return nil
			})
			if err != ErrCircuitOpen {
				// This goroutine got through
				assert.NoError(t, err)
			}
		}()
	}
	
	wg.Wait()
	
	// At most 1 transition call + HalfOpenMaxAttempts calls should be allowed
	// 1 call triggers Open->HalfOpen transition (doesn't count against attempts)
	// Then HalfOpenMaxAttempts = 2 more calls are allowed in half-open state
	// Total: 1 + 2 = 3 calls should succeed
	assert.LessOrEqual(t, int(attempts.Load()), 1+config.HalfOpenMaxAttempts)
}

func TestCircuitBreaker_ObservationWindow(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   200 * time.Millisecond,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Add 2 failures
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	
	// Circuit should still be closed (need 3 failures)
	assert.Equal(t, StateClosed, cb.GetState())
	
	// Wait for observation window to pass
	time.Sleep(250 * time.Millisecond)
	
	// Old failures should be expired, add 2 more
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	
	// Circuit should still be closed (old failures expired)
	assert.Equal(t, StateClosed, cb.GetState())
	
	// Add one more failure quickly
	cb.Call(ctx, func() error {
		return testErr
	})
	
	// Now circuit should open (3 failures within window)
	assert.Equal(t, StateOpen, cb.GetState())
}

func TestCircuitBreaker_StateChangeCallback(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	var transitions []string
	var mu sync.Mutex
	
	cb.SetOnStateChange(func(from, to CircuitState) {
		mu.Lock()
		defer mu.Unlock()
		transitions = append(transitions, from.String()+"->"+to.String())
	})
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Trigger state changes
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	
	time.Sleep(150 * time.Millisecond)
	
	cb.Call(ctx, func() error {
		return nil
	})
	
	mu.Lock()
	defer mu.Unlock()
	
	assert.Contains(t, transitions, "closed->open")
	assert.Contains(t, transitions, "open->half-open")
	assert.Contains(t, transitions, "half-open->closed")
}

func TestCircuitBreaker_Reset(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	assert.Equal(t, StateOpen, cb.GetState())
	
	// Reset should close the circuit
	cb.Reset()
	assert.Equal(t, StateClosed, cb.GetState())
	
	// Should be able to make calls again
	err = cb.Call(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
}

func TestCircuitBreaker_GetStats(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	testErr := errors.New("test error")
	
	// Generate some activity
	cb.Call(ctx, func() error {
		return nil
	})
	
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func() error {
			return testErr
		})
	}
	
	stats := cb.GetStats()
	
	assert.Equal(t, StateClosed, stats.State)
	assert.Equal(t, 2, stats.ConsecutiveFailures)
	assert.Equal(t, 0, stats.ConsecutiveSuccesses)
	assert.Equal(t, 2, stats.FailuresInWindow)
	assert.NotZero(t, stats.LastStateChange)
}

func TestCircuitBreaker_ConcurrentCalls(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    3,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 3,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	ctx := context.Background()
	
	// Run many concurrent calls
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var failureCount atomic.Int32
	
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			
			err := cb.Call(ctx, func() error {
				if i%3 == 0 {
					return errors.New("error")
				}
				return nil
			})
			
			if err == nil {
				successCount.Add(1)
			} else if err != ErrCircuitOpen {
				failureCount.Add(1)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Should have processed some calls
	assert.Greater(t, int(successCount.Load()+failureCount.Load()), 0)
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state    CircuitState
		expected string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}
	
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestCircuitBreaker_ContextCancellation(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxAttempts: 2,
		ObservationWindow:   1 * time.Second,
	}
	
	cb, err := NewCircuitBreaker(config)
	require.NoError(t, err)
	
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	
	// Call should still execute (circuit breaker doesn't check context)
	// The function itself would need to check context
	err = cb.Call(ctx, func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})
	
	assert.Equal(t, context.Canceled, err)
}