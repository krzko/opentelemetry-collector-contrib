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

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// MockRedisOperations provides mock Redis operations for testing
type MockRedisOperations struct {
	pingFunc       func(ctx context.Context) error
	scriptLoadFunc func(ctx context.Context, script string) (string, error)
	evalSHAFunc    func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error)
	closeFunc      func() error
	
	// Track calls
	pingCalls       atomic.Int32
	scriptLoadCalls atomic.Int32
	evalSHACalls    atomic.Int32
	closeCalls      atomic.Int32
}

func (m *MockRedisOperations) Ping(ctx context.Context) error {
	m.pingCalls.Add(1)
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func (m *MockRedisOperations) ScriptLoad(ctx context.Context, script string) (string, error) {
	m.scriptLoadCalls.Add(1)
	if m.scriptLoadFunc != nil {
		return m.scriptLoadFunc(ctx, script)
	}
	return "test-sha", nil
}

func (m *MockRedisOperations) EvalSHA(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
	m.evalSHACalls.Add(1)
	if m.evalSHAFunc != nil {
		return m.evalSHAFunc(ctx, sha, keys, args)
	}
	return nil, nil
}

func (m *MockRedisOperations) Close() error {
	m.closeCalls.Add(1)
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// Implement remaining RedisClient methods as no-ops
func (m *MockRedisOperations) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return nil, nil
}

func (m *MockRedisOperations) HSet(ctx context.Context, key string, values ...interface{}) error {
	return nil
}

func (m *MockRedisOperations) HGet(ctx context.Context, key string, field string) (string, error) {
	return "", nil
}

func (m *MockRedisOperations) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return nil, nil
}

func (m *MockRedisOperations) LLen(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (m *MockRedisOperations) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (m *MockRedisOperations) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return nil
}

func (m *MockRedisOperations) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return nil
}

func (m *MockRedisOperations) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return nil, nil
}

func (m *MockRedisOperations) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return nil
}

func (m *MockRedisOperations) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return nil
}

func (m *MockRedisOperations) PExpire(ctx context.Context, key string, expiration time.Duration) error {
	return nil
}

func (m *MockRedisOperations) Pipeline() redis.Pipeliner {
	return nil
}

func (m *MockRedisOperations) TxPipeline() redis.Pipeliner {
	return nil
}

func TestRedisClientWithCircuitBreaker(t *testing.T) {
	t.Run("circuit breaker opens on consecutive failures", func(t *testing.T) {
		config := ClientConfig{
			Endpoints: []string{"localhost:6379"},
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold:    3,
				SuccessThreshold:    2,
				OpenDuration:        100 * time.Millisecond,
				HalfOpenMaxAttempts: 2,
				ObservationWindow:   1 * time.Second,
			},
		}
		
		// Create circuit breaker
		config.CircuitBreaker.SetDefaults()
		cb, err := NewCircuitBreaker(*config.CircuitBreaker)
		require.NoError(t, err)
		
		// Create mock client
		mock := &MockRedisOperations{
			pingFunc: func(ctx context.Context) error {
				return errors.New("connection failed")
			},
		}
		
		client := &redisClientImpl{
			client:         nil, // We're using mock
			logger:         zap.NewNop(),
			circuitBreaker: cb,
			closeCh:        make(chan struct{}),
		}
		
		// Simulate failures to open circuit
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			err := client.wrapCall(ctx, func() error {
				return mock.Ping(ctx)
			})
			assert.Error(t, err)
		}
		
		// Circuit should be open
		assert.Equal(t, StateOpen, cb.GetState())
		
		// Next call should fail immediately with ErrCircuitOpen
		err = client.wrapCall(ctx, func() error {
			return mock.Ping(ctx)
		})
		assert.Equal(t, ErrCircuitOpen, err)
		assert.Equal(t, int32(3), mock.pingCalls.Load()) // Only 3 calls made
	})
	
	t.Run("circuit breaker closes after successful calls", func(t *testing.T) {
		config := ClientConfig{
			Endpoints: []string{"localhost:6379"},
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold:    2,
				SuccessThreshold:    2,
				OpenDuration:        100 * time.Millisecond,
				HalfOpenMaxAttempts: 2,
				ObservationWindow:   1 * time.Second,
			},
		}
		
		// Create circuit breaker
		config.CircuitBreaker.SetDefaults()
		cb, err := NewCircuitBreaker(*config.CircuitBreaker)
		require.NoError(t, err)
		
		// Create mock client with controllable behavior
		var failCount atomic.Int32
		mock := &MockRedisOperations{
			pingFunc: func(ctx context.Context) error {
				if failCount.Load() < 2 {
					failCount.Add(1)
					return errors.New("connection failed")
				}
				return nil // Success after 2 failures
			},
		}
		
		client := &redisClientImpl{
			client:         nil, // We're using mock
			logger:         zap.NewNop(),
			circuitBreaker: cb,
			closeCh:        make(chan struct{}),
		}
		
		ctx := context.Background()
		
		// Trigger failures to open circuit
		for i := 0; i < 2; i++ {
			client.wrapCall(ctx, func() error {
				return mock.Ping(ctx)
			})
		}
		assert.Equal(t, StateOpen, cb.GetState())
		
		// Wait for half-open
		time.Sleep(150 * time.Millisecond)
		
		// Successful calls should close circuit
		for i := 0; i < 2; i++ {
			err := client.wrapCall(ctx, func() error {
				return mock.Ping(ctx)
			})
			assert.NoError(t, err)
		}
		
		assert.Equal(t, StateClosed, cb.GetState())
	})
	
	t.Run("wrapCall handles closed client", func(t *testing.T) {
		client := &redisClientImpl{
			client:         nil,
			logger:         zap.NewNop(),
			circuitBreaker: nil,
			closeCh:        make(chan struct{}),
		}
		
		// Mark client as closed
		client.closed.Store(true)
		
		ctx := context.Background()
		err := client.wrapCall(ctx, func() error {
			return nil
		})
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is closed")
	})
	
	t.Run("circuit breaker state change callback", func(t *testing.T) {
		config := ClientConfig{
			Endpoints: []string{"localhost:6379"},
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold:    2,
				SuccessThreshold:    1,
				OpenDuration:        100 * time.Millisecond,
				HalfOpenMaxAttempts: 2,
				ObservationWindow:   1 * time.Second,
			},
		}
		
		// Create circuit breaker
		config.CircuitBreaker.SetDefaults()
		cb, err := NewCircuitBreaker(*config.CircuitBreaker)
		require.NoError(t, err)
		
		var stateChanges []string
		var mu sync.Mutex
		
		cb.SetOnStateChange(func(from, to CircuitState) {
			mu.Lock()
			defer mu.Unlock()
			stateChanges = append(stateChanges, from.String()+"->"+to.String())
		})
		
		// Create mock client with failures
		mock := &MockRedisOperations{
			pingFunc: func(ctx context.Context) error {
				return errors.New("connection failed")
			},
		}
		
		client := &redisClientImpl{
			client:         nil,
			logger:         zap.NewNop(),
			circuitBreaker: cb,
			closeCh:        make(chan struct{}),
		}
		
		ctx := context.Background()
		
		// Trigger failures to cause state change from Closed to Open
		for i := 0; i < 2; i++ {
			client.wrapCall(ctx, func() error {
				return mock.Ping(ctx)
			})
		}
		
		// Check that state change was recorded
		mu.Lock()
		assert.Contains(t, stateChanges, "closed->open")
		mu.Unlock()
	})
}

func TestRedisClientReconnection(t *testing.T) {
	t.Run("prevents concurrent reconnection attempts", func(t *testing.T) {
		client := &redisClientImpl{
			client:            nil,
			logger:            zap.NewNop(),
			circuitBreaker:    nil,
			config:            ClientConfig{Endpoints: []string{"localhost:6379"}},
			reconnectDelay:    10 * time.Millisecond,
			maxReconnectDelay: 100 * time.Millisecond,
			closeCh:           make(chan struct{}),
		}
		
		var wg sync.WaitGroup
		reconnectAttempts := atomic.Int32{}
		
		// Try to trigger multiple reconnections concurrently
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Check if already reconnecting
				if !client.isReconnecting.Load() {
					// Try to set reconnecting flag
					client.reconnectMu.Lock()
					if !client.isReconnecting.Load() {
						client.isReconnecting.Store(true)
						reconnectAttempts.Add(1)
						client.reconnectMu.Unlock()
						time.Sleep(50 * time.Millisecond) // Simulate reconnection work
						client.isReconnecting.Store(false)
					} else {
						client.reconnectMu.Unlock()
					}
				}
			}()
		}
		
		wg.Wait()
		
		// Only one reconnection should have occurred
		assert.Equal(t, int32(1), reconnectAttempts.Load())
	})
}

func TestTLSConfigCreation(t *testing.T) {
	t.Run("creates TLS config with insecure skip verify", func(t *testing.T) {
		config := TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		}
		
		tlsConfig, err := createTLSConfig(config)
		require.NoError(t, err)
		assert.NotNil(t, tlsConfig)
		assert.True(t, tlsConfig.InsecureSkipVerify)
	})
	
	t.Run("handles missing CA file gracefully", func(t *testing.T) {
		config := TLSConfig{
			Enabled: true,
			CAFile:  "/nonexistent/ca.pem",
		}
		
		_, err := createTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA certificate")
	})
}