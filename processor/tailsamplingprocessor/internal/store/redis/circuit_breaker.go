// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitState represents the state of the circuit breaker
type CircuitState int32

const (
	// StateClosed allows all operations (normal operation)
	StateClosed CircuitState = iota
	// StateOpen blocks all operations (failure detected)
	StateOpen
	// StateHalfOpen allows limited operations (recovery testing)
	StateHalfOpen
)

// String returns the string representation of the circuit state
func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig configures the circuit breaker behavior
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures to open the circuit
	FailureThreshold int `mapstructure:"failure_threshold"`

	// SuccessThreshold is the number of consecutive successes to close the circuit
	SuccessThreshold int `mapstructure:"success_threshold"`

	// OpenDuration is how long the circuit stays open before moving to half-open
	OpenDuration time.Duration `mapstructure:"open_duration"`

	// HalfOpenMaxAttempts is the max operations allowed in half-open state
	HalfOpenMaxAttempts int `mapstructure:"half_open_max_attempts"`

	// ObservationWindow is the time window for tracking failures
	ObservationWindow time.Duration `mapstructure:"observation_window"`
}

// SetDefaults sets default values for circuit breaker configuration
func (c *CircuitBreakerConfig) SetDefaults() {
	if c.FailureThreshold == 0 {
		c.FailureThreshold = 5
	}
	if c.SuccessThreshold == 0 {
		c.SuccessThreshold = 3
	}
	if c.OpenDuration == 0 {
		c.OpenDuration = 30 * time.Second
	}
	if c.HalfOpenMaxAttempts == 0 {
		c.HalfOpenMaxAttempts = 3
	}
	if c.ObservationWindow == 0 {
		c.ObservationWindow = 1 * time.Minute
	}
}

// Validate validates the circuit breaker configuration
func (c *CircuitBreakerConfig) Validate() error {
	if c.FailureThreshold < 1 {
		return errors.New("failure_threshold must be at least 1")
	}
	if c.SuccessThreshold < 1 {
		return errors.New("success_threshold must be at least 1")
	}
	if c.OpenDuration <= 0 {
		return errors.New("open_duration must be positive")
	}
	if c.HalfOpenMaxAttempts < 1 {
		return errors.New("half_open_max_attempts must be at least 1")
	}
	if c.ObservationWindow <= 0 {
		return errors.New("observation_window must be positive")
	}
	return nil
}

// CircuitBreaker implements the circuit breaker pattern for Redis operations
type CircuitBreaker struct {
	config CircuitBreakerConfig

	// State management
	state           atomic.Int32 // CircuitState
	lastStateChange time.Time
	stateMu         sync.RWMutex

	// Failure tracking
	consecutiveFailures  atomic.Int32
	consecutiveSuccesses atomic.Int32
	halfOpenAttempts     atomic.Int32

	// Failure history for observation window
	failureHistory []time.Time
	historyMu      sync.Mutex

	// Callbacks
	onStateChange func(from, to CircuitState)
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig) (*CircuitBreaker, error) {
	// Validate the config first to catch invalid values
	if err := config.Validate(); err != nil {
		// Only set defaults if validation fails due to zero values
		// Check if all values are zero (uninitialized)
		if config.FailureThreshold == 0 && config.SuccessThreshold == 0 && 
			config.OpenDuration == 0 && config.HalfOpenMaxAttempts == 0 && 
			config.ObservationWindow == 0 {
			// All zeros means uninitialized, set defaults
			config.SetDefaults()
			// Validate again after setting defaults
			if err := config.Validate(); err != nil {
				return nil, err
			}
		} else {
			// Some values were set but invalid
			return nil, err
		}
	}

	cb := &CircuitBreaker{
		config:          config,
		lastStateChange: time.Now(),
		failureHistory:  make([]time.Time, 0, config.FailureThreshold),
	}
	cb.state.Store(int32(StateClosed))

	return cb, nil
}

// Call executes the given function with circuit breaker protection
func (cb *CircuitBreaker) Call(ctx context.Context, fn func() error) error {
	if !cb.canAttempt() {
		return ErrCircuitOpen
	}

	// Execute the function
	err := fn()

	// Update circuit breaker state based on result
	if err != nil {
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}

	return err
}

// canAttempt checks if an operation can be attempted
func (cb *CircuitBreaker) canAttempt() bool {
	state := CircuitState(cb.state.Load())

	switch state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if we should transition to half-open (with mutex to prevent race)
		cb.stateMu.Lock()
		defer cb.stateMu.Unlock()
		
		// Re-check state after acquiring lock in case another goroutine already transitioned
		currentState := CircuitState(cb.state.Load())
		if currentState != StateOpen {
			// State changed while waiting for lock
			if currentState == StateHalfOpen {
				// Now in half-open, check attempts
				attempts := cb.halfOpenAttempts.Add(1)
				return attempts <= int32(cb.config.HalfOpenMaxAttempts)
			}
			return currentState == StateClosed
		}
		
		shouldTransition := time.Since(cb.lastStateChange) >= cb.config.OpenDuration
		if shouldTransition {
			// Transition to half-open and allow this call (doesn't count against attempts)
			cb.state.Store(int32(StateHalfOpen))
			cb.lastStateChange = time.Now()
			cb.halfOpenAttempts.Store(0)
			cb.consecutiveSuccesses.Store(0)
			
			// Notify state change
			if cb.onStateChange != nil {
				cb.onStateChange(StateOpen, StateHalfOpen)
			}
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited attempts in half-open state
		attempts := cb.halfOpenAttempts.Add(1)
		return attempts <= int32(cb.config.HalfOpenMaxAttempts)

	default:
		return false
	}
}

// recordFailure records a failure and potentially opens the circuit
func (cb *CircuitBreaker) recordFailure() {
	state := CircuitState(cb.state.Load())

	// Add to failure history
	cb.addFailureToHistory()

	// Reset consecutive successes
	cb.consecutiveSuccesses.Store(0)

	// Increment consecutive failures
	failures := cb.consecutiveFailures.Add(1)

	switch state {
	case StateClosed:
		// Check if we should open the circuit
		if cb.shouldOpen() {
			cb.transitionTo(StateOpen)
		}

	case StateHalfOpen:
		// Any failure in half-open state reopens the circuit
		cb.transitionTo(StateOpen)
	}

	// Log if we're approaching the threshold
	if state == StateClosed && failures == int32(cb.config.FailureThreshold-1) {
		// One more failure will open the circuit
	}
}

// recordSuccess records a success and potentially closes the circuit
func (cb *CircuitBreaker) recordSuccess() {
	state := CircuitState(cb.state.Load())

	// Reset consecutive failures
	cb.consecutiveFailures.Store(0)

	// Increment consecutive successes
	successes := cb.consecutiveSuccesses.Add(1)

	switch state {
	case StateHalfOpen:
		// Check if we should close the circuit
		if successes >= int32(cb.config.SuccessThreshold) {
			cb.transitionTo(StateClosed)
		}
	}
}

// shouldOpen checks if the circuit should open based on failure history
func (cb *CircuitBreaker) shouldOpen() bool {
	cb.historyMu.Lock()
	defer cb.historyMu.Unlock()

	// Clean old entries outside observation window
	now := time.Now()
	cutoff := now.Add(-cb.config.ObservationWindow)

	validHistory := make([]time.Time, 0, len(cb.failureHistory))
	for _, t := range cb.failureHistory {
		if t.After(cutoff) {
			validHistory = append(validHistory, t)
		}
	}
	cb.failureHistory = validHistory

	// Check if we have enough failures in the window
	return len(cb.failureHistory) >= cb.config.FailureThreshold
}

// addFailureToHistory adds a failure timestamp to the history
func (cb *CircuitBreaker) addFailureToHistory() {
	cb.historyMu.Lock()
	defer cb.historyMu.Unlock()

	cb.failureHistory = append(cb.failureHistory, time.Now())
}

// transitionTo transitions the circuit breaker to a new state
func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	oldState := CircuitState(cb.state.Swap(int32(newState)))

	if oldState == newState {
		return // No transition
	}

	cb.stateMu.Lock()
	cb.lastStateChange = time.Now()
	cb.stateMu.Unlock()

	// Reset counters based on transition
	switch newState {
	case StateOpen:
		cb.consecutiveFailures.Store(0)
		cb.consecutiveSuccesses.Store(0)

	case StateHalfOpen:
		cb.halfOpenAttempts.Store(0)
		cb.consecutiveSuccesses.Store(0)

	case StateClosed:
		cb.consecutiveFailures.Store(0)
		cb.consecutiveSuccesses.Store(0)
		cb.halfOpenAttempts.Store(0)
		// Clear failure history on successful close
		cb.historyMu.Lock()
		cb.failureHistory = cb.failureHistory[:0]
		cb.historyMu.Unlock()
	}

	// Notify state change
	if cb.onStateChange != nil {
		cb.onStateChange(oldState, newState)
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitState {
	return CircuitState(cb.state.Load())
}

// SetOnStateChange sets the callback for state changes
func (cb *CircuitBreaker) SetOnStateChange(fn func(from, to CircuitState)) {
	cb.onStateChange = fn
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.transitionTo(StateClosed)
}

// GetStats returns current statistics
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	cb.stateMu.RLock()
	lastChange := cb.lastStateChange
	cb.stateMu.RUnlock()

	cb.historyMu.Lock()
	failureCount := len(cb.failureHistory)
	cb.historyMu.Unlock()

	return CircuitBreakerStats{
		State:                cb.GetState(),
		ConsecutiveFailures:  int(cb.consecutiveFailures.Load()),
		ConsecutiveSuccesses: int(cb.consecutiveSuccesses.Load()),
		FailuresInWindow:     failureCount,
		LastStateChange:      lastChange,
	}
}

// CircuitBreakerStats contains circuit breaker statistics
type CircuitBreakerStats struct {
	State                CircuitState
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	FailuresInWindow     int
	LastStateChange      time.Time
}
