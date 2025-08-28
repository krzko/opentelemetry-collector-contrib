// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// MockRedisClient implements RedisClient for testing
type MockRedisClient struct {
	scriptLoadFunc func(ctx context.Context, script string) (string, error)
	evalSHAFunc    func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error)
	
	// Track calls
	scriptLoadCalls atomic.Int32
	evalSHACalls    atomic.Int32
}

func (m *MockRedisClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	m.scriptLoadCalls.Add(1)
	if m.scriptLoadFunc != nil {
		return m.scriptLoadFunc(ctx, script)
	}
	return "test-sha", nil
}

func (m *MockRedisClient) EvalSHA(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
	m.evalSHACalls.Add(1)
	if m.evalSHAFunc != nil {
		return m.evalSHAFunc(ctx, sha, keys, args)
	}
	return nil, nil
}

// Implement other RedisClient methods as no-ops
func (m *MockRedisClient) Ping(ctx context.Context) error { return nil }
func (m *MockRedisClient) Close() error { return nil }
func (m *MockRedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return nil, nil
}
func (m *MockRedisClient) HSet(ctx context.Context, key string, values ...interface{}) error {
	return nil
}
func (m *MockRedisClient) HGet(ctx context.Context, key string, field string) (string, error) {
	return "", nil
}
func (m *MockRedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return nil, nil
}
func (m *MockRedisClient) LLen(ctx context.Context, key string) (int64, error) {
	return 0, nil
}
func (m *MockRedisClient) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}
func (m *MockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return nil
}
func (m *MockRedisClient) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return nil
}
func (m *MockRedisClient) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return nil, nil
}
func (m *MockRedisClient) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return nil
}
func (m *MockRedisClient) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return nil
}
func (m *MockRedisClient) PExpire(ctx context.Context, key string, expiration time.Duration) error {
	return nil
}
func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	return nil
}
func (m *MockRedisClient) TxPipeline() redis.Pipeliner {
	return nil
}

func TestNewScriptRetryManager(t *testing.T) {
	client := &MockRedisClient{}
	config := RetryConfig{
		Enabled:           true,
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
	}
	
	srm := NewScriptRetryManager(client, config, nil)
	assert.NotNil(t, srm)
	assert.Equal(t, client, srm.client)
	assert.Equal(t, config, srm.config)
	assert.NotNil(t, srm.logger)
}

func TestScriptRetryManager_LoadScript(t *testing.T) {
	t.Run("successful load", func(t *testing.T) {
		client := &MockRedisClient{
			scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
				return "test-sha-123", nil
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		sha, err := srm.LoadScript(ctx, "test-script", "return 1")
		
		require.NoError(t, err)
		assert.Equal(t, "test-sha-123", sha)
		assert.Equal(t, int32(1), client.scriptLoadCalls.Load())
		
		// Verify script is cached
		cachedSHA, exists := srm.scriptSHAs["test-script"]
		assert.True(t, exists)
		assert.Equal(t, "test-sha-123", cachedSHA)
	})
	
	t.Run("load with retry", func(t *testing.T) {
		callCount := 0
		client := &MockRedisClient{
			scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
				callCount++
				if callCount < 3 {
					return "", errors.New("connection error")
				}
				return "test-sha-456", nil
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		sha, err := srm.LoadScript(ctx, "test-script", "return 1")
		
		require.NoError(t, err)
		assert.Equal(t, "test-sha-456", sha)
		assert.Equal(t, 3, callCount)
	})
	
	t.Run("load failure after max attempts", func(t *testing.T) {
		client := &MockRedisClient{
			scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
				return "", errors.New("persistent error")
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       2,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		sha, err := srm.LoadScript(ctx, "test-script", "return 1")
		
		require.Error(t, err)
		assert.Contains(t, err.Error(), "persistent error")
		assert.Empty(t, sha)
		assert.Equal(t, int32(2), client.scriptLoadCalls.Load())
	})
}

func TestScriptRetryManager_ExecuteScript(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		client := &MockRedisClient{
			scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
				return "test-sha", nil
			},
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				return "result", nil
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		
		// Load script first
		_, err := srm.LoadScript(ctx, "test-script", "return 1")
		require.NoError(t, err)
		
		// Execute script
		result, err := srm.ExecuteScript(ctx, "test-script", []string{"key1"}, []interface{}{"arg1"})
		
		require.NoError(t, err)
		assert.Equal(t, "result", result)
		assert.Equal(t, int32(1), client.evalSHACalls.Load())
	})
	
	t.Run("NOSCRIPT error with reload", func(t *testing.T) {
		evalCallCount := 0
		client := &MockRedisClient{
			scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
				return "new-sha", nil
			},
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				evalCallCount++
				if evalCallCount == 1 {
					return nil, errors.New("NOSCRIPT No matching script")
				}
				return "result-after-reload", nil
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		
		// Pre-load script with old SHA
		srm.scriptSHAs["test-script"] = "old-sha"
		srm.scripts["test-script"] = "return 1"
		
		// Execute script - should get NOSCRIPT, reload, and retry
		result, err := srm.ExecuteScript(ctx, "test-script", []string{"key1"}, []interface{}{"arg1"})
		
		require.NoError(t, err)
		assert.Equal(t, "result-after-reload", result)
		assert.Equal(t, 2, evalCallCount)
		assert.Equal(t, int32(1), client.scriptLoadCalls.Load()) // Script was reloaded
		assert.Equal(t, "new-sha", srm.scriptSHAs["test-script"])
	})
	
	t.Run("retryable error with backoff", func(t *testing.T) {
		evalCallCount := 0
		client := &MockRedisClient{
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				evalCallCount++
				if evalCallCount < 3 {
					return nil, errors.New("connection timeout")
				}
				return "success", nil
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		
		// Pre-load script
		srm.scriptSHAs["test-script"] = "test-sha"
		srm.scripts["test-script"] = "return 1"
		
		start := time.Now()
		result, err := srm.ExecuteScript(ctx, "test-script", nil, nil)
		elapsed := time.Since(start)
		
		require.NoError(t, err)
		assert.Equal(t, "success", result)
		assert.Equal(t, 3, evalCallCount)
		// Should have waited at least initial + (initial * multiplier)
		assert.Greater(t, elapsed, 20*time.Millisecond)
	})
	
	t.Run("non-retryable error", func(t *testing.T) {
		client := &MockRedisClient{
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		
		// Pre-load script
		srm.scriptSHAs["test-script"] = "test-sha"
		srm.scripts["test-script"] = "return 1"
		
		result, err := srm.ExecuteScript(ctx, "test-script", nil, nil)
		
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-retryable")
		assert.Contains(t, err.Error(), "WRONGTYPE")
		assert.Nil(t, result)
		assert.Equal(t, int32(1), client.evalSHACalls.Load()) // Only one attempt
	})
	
	t.Run("script not loaded", func(t *testing.T) {
		client := &MockRedisClient{}
		config := RetryConfig{Enabled: true}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		result, err := srm.ExecuteScript(ctx, "unknown-script", nil, nil)
		
		require.Error(t, err)
		assert.Contains(t, err.Error(), "script unknown-script not loaded")
		assert.Nil(t, result)
	})
	
	t.Run("circuit breaker open error", func(t *testing.T) {
		client := &MockRedisClient{
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				return nil, ErrCircuitOpen
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx := context.Background()
		
		// Pre-load script
		srm.scriptSHAs["test-script"] = "test-sha"
		srm.scripts["test-script"] = "return 1"
		
		result, err := srm.ExecuteScript(ctx, "test-script", nil, nil)
		
		require.Error(t, err)
		assert.Equal(t, ErrCircuitOpen, err)
		assert.Nil(t, result)
		assert.Equal(t, int32(1), client.evalSHACalls.Load()) // No retry on circuit open
	})
	
	t.Run("context cancellation", func(t *testing.T) {
		client := &MockRedisClient{
			evalSHAFunc: func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
				return nil, errors.New("timeout")
			},
		}
		
		config := RetryConfig{
			Enabled:           true,
			MaxAttempts:       5,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        1 * time.Second,
			BackoffMultiplier: 2.0,
		}
		
		srm := NewScriptRetryManager(client, config, zap.NewNop())
		
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		
		// Pre-load script
		srm.scriptSHAs["test-script"] = "test-sha"
		srm.scripts["test-script"] = "return 1"
		
		result, err := srm.ExecuteScript(ctx, "test-script", nil, nil)
		
		require.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Nil(t, result)
	})
}

func TestScriptRetryManager_LoadAllScripts(t *testing.T) {
	loadedScripts := make(map[string]string)
	
	client := &MockRedisClient{
		scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
			sha := fmt.Sprintf("sha-%d", len(loadedScripts))
			loadedScripts[script] = sha
			return sha, nil
		},
	}
	
	config := RetryConfig{
		Enabled:           true,
		MaxAttempts:       3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}
	
	srm := NewScriptRetryManager(client, config, zap.NewNop())
	
	ctx := context.Background()
	err := srm.LoadAllScripts(ctx)
	
	require.NoError(t, err)
	
	// Should have loaded all scripts
	expectedScripts := []string{
		"append_and_index",
		"lease_acquire",
		"lease_release",
		"decision_set",
		"spill_add_segment",
		"complete_and_gc",
	}
	
	for _, name := range expectedScripts {
		sha, exists := srm.scriptSHAs[name]
		assert.True(t, exists, "Script %s should be loaded", name)
		assert.NotEmpty(t, sha)
	}
	
	assert.Equal(t, len(expectedScripts), len(loadedScripts))
}

func TestIsNoScriptError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "NOSCRIPT error",
			err:      errors.New("NOSCRIPT No matching script"),
			expected: true,
		},
		{
			name:     "No matching script error",
			err:      errors.New("ERR No matching script found"),
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("connection timeout"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNoScriptError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// Non-retryable errors
		{
			name:     "WRONGTYPE error",
			err:      errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"),
			expected: false,
		},
		{
			name:     "ERR error",
			err:      errors.New("ERR invalid argument"),
			expected: false,
		},
		{
			name:     "READONLY error",
			err:      errors.New("READONLY You can't write against a read only replica"),
			expected: false,
		},
		{
			name:     "context cancelled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "redis nil",
			err:      redis.Nil,
			expected: false,
		},
		{
			name:     "NOSCRIPT error",
			err:      errors.New("NOSCRIPT No matching script"),
			expected: false,
		},
		// Retryable errors
		{
			name:     "connection error",
			err:      errors.New("connection refused"),
			expected: true,
		},
		{
			name:     "timeout error",
			err:      errors.New("i/o timeout"),
			expected: true,
		},
		{
			name:     "EOF error",
			err:      errors.New("unexpected EOF"),
			expected: true,
		},
		{
			name:     "LOADING error",
			err:      errors.New("LOADING Redis is loading the dataset in memory"),
			expected: true,
		},
		{
			name:     "BUSY error",
			err:      errors.New("BUSY Redis is busy"),
			expected: true,
		},
		{
			name:     "CLUSTERDOWN error",
			err:      errors.New("CLUSTERDOWN The cluster is down"),
			expected: true,
		},
		{
			name:     "unknown error",
			err:      errors.New("some unknown error"),
			expected: true,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScriptRetryManager_DisabledRetries(t *testing.T) {
	callCount := 0
	client := &MockRedisClient{
		scriptLoadFunc: func(ctx context.Context, script string) (string, error) {
			callCount++
			return "", errors.New("error")
		},
	}
	
	config := RetryConfig{
		Enabled: false, // Retries disabled
	}
	
	srm := NewScriptRetryManager(client, config, zap.NewNop())
	
	ctx := context.Background()
	sha, err := srm.LoadScript(ctx, "test-script", "return 1")
	
	require.Error(t, err)
	assert.Empty(t, sha)
	assert.Equal(t, 1, callCount) // Should only try once
}