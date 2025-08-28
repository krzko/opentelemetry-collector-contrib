// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

func TestNewRedisStore_BasicConfiguration(t *testing.T) {
	config := NewDefaultConfig()
	hotLimits := store.Config{
		HotLimits: store.HotLimits{
			MaxSpanPerTrace:  1000,
			MaxBytesPerTrace: 1024 * 1024,
			DefaultTTL:       time.Minute,
			LeaseTTL:         time.Second * 10,
		},
	}
	
	// Since we don't have a real Redis instance for testing,
	// we'll test configuration validation instead
	err := config.Validate()
	require.NoError(t, err, "Default config should be valid")
	
	assert.Equal(t, "ts:", config.Keyspace)
	assert.Equal(t, 10*time.Minute, config.DefaultTTL)
	assert.Equal(t, 30*time.Second, config.DecisionWait)
	assert.Equal(t, time.Hour, config.DecisionTTL)
	assert.Equal(t, 10*time.Second, config.LeaseTTL)
	assert.False(t, config.EnableAOF)
	
	// Test client config
	assert.Equal(t, []string{"localhost:6379"}, config.Client.Endpoints)
	assert.False(t, config.Client.Cluster)
	assert.Equal(t, 0, config.Client.Database)
	assert.Equal(t, 10, config.Client.Pool.MaxIdleConns)
	assert.Equal(t, 100, config.Client.Pool.MaxActiveConns)
	assert.Equal(t, time.Hour, config.Client.Pool.ConnMaxLifetime)
	assert.Equal(t, 30*time.Minute, config.Client.Pool.ConnMaxIdleTime)
	
	// Test timeout config
	assert.Equal(t, 5*time.Second, config.Client.Timeouts.Connect)
	assert.Equal(t, 3*time.Second, config.Client.Timeouts.Read)
	assert.Equal(t, 3*time.Second, config.Client.Timeouts.Write)
	
	// Test retry config
	assert.True(t, config.Retry.Enabled)
	assert.Equal(t, 3, config.Retry.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, config.Retry.InitialBackoff)
	assert.Equal(t, 5*time.Second, config.Retry.MaxBackoff)
	assert.Equal(t, 2.0, config.Retry.BackoffMultiplier)
	
	t.Logf("Redis store configuration validated successfully")
	t.Logf("Hot limits: MaxSpanPerTrace=%d, MaxBytesPerTrace=%d", 
		hotLimits.HotLimits.MaxSpanPerTrace, hotLimits.HotLimits.MaxBytesPerTrace)
}

func TestRedisConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid default config",
			config:  NewDefaultConfig(),
			wantErr: false,
		},
		{
			name: "empty keyspace",
			config: Config{
				Keyspace:     "",
				DefaultTTL:   time.Minute,
				DecisionWait: time.Second * 30,
				DecisionTTL:  time.Hour,
				LeaseTTL:     time.Second * 10,
				Client:       NewDefaultConfig().Client,
				Retry:        NewDefaultConfig().Retry,
			},
			wantErr: true,
			errMsg:  "keyspace cannot be empty",
		},
		{
			name: "negative TTL",
			config: Config{
				Keyspace:     "ts:",
				DefaultTTL:   -time.Minute,
				DecisionWait: time.Second * 30,
				DecisionTTL:  time.Hour,
				LeaseTTL:     time.Second * 10,
				Client:       NewDefaultConfig().Client,
				Retry:        NewDefaultConfig().Retry,
			},
			wantErr: true,
			errMsg:  "default_ttl must be positive",
		},
		{
			name: "no endpoints",
			config: Config{
				Keyspace:     "ts:",
				DefaultTTL:   time.Minute,
				DecisionWait: time.Second * 30,
				DecisionTTL:  time.Hour,
				LeaseTTL:     time.Second * 10,
				Client: ClientConfig{
					Endpoints: []string{},
				},
				Retry: NewDefaultConfig().Retry,
			},
			wantErr: true,
			errMsg:  "at least one Redis endpoint must be specified",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTraceKeysGeneration(t *testing.T) {
	config := NewDefaultConfig()
	hotLimits := store.Config{
		HotLimits: store.HotLimits{
			MaxSpanPerTrace:  1000,
			MaxBytesPerTrace: 1024 * 1024,
		},
	}
	
	// Create a mock Redis store (without actual Redis connection)
	rs := &RedisStore{
		logger:    zap.NewNop(),
		keyspace:  config.Keyspace,
		hotLimits: hotLimits,
		stats:     StoreStats{},
	}
	
	// Test trace key generation
	traceID := store.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	keys := rs.generateTraceKeys(traceID)
	
	expectedPrefix := "ts:trace:0102030405060708090a0b0c0d0e0f10"
	assert.Equal(t, expectedPrefix+":meta", keys.Meta)
	assert.Equal(t, expectedPrefix+":spans", keys.Spans)
	assert.Equal(t, expectedPrefix+":dedup", keys.Dedup)
	assert.Equal(t, expectedPrefix+":segments", keys.Segments)
	assert.Equal(t, expectedPrefix+":decision", keys.Decision)
	assert.Equal(t, expectedPrefix+":lease", keys.Lease)
	assert.Equal(t, "ts:active", keys.Active)
	
	t.Logf("Generated Redis keys for trace %x:", traceID)
	t.Logf("  Meta: %s", keys.Meta)
	t.Logf("  Spans: %s", keys.Spans)
	t.Logf("  Decision: %s", keys.Decision)
	t.Logf("  Active: %s", keys.Active)
}

func TestStoreStats_Calculation(t *testing.T) {
	stats := StoreStats{
		AppendCalls:       100,
		AppendErrors:      5,
		FetchAllCalls:     50,
		FetchAllErrors:    2,
		LeaseCalls:        20,
		LeaseErrors:       1,
		DecisionSetCalls:  30,
		DecisionSetErrors: 0,
		DecisionGetCalls:  40,
		DecisionGetErrors: 1,
		TracesActive:      10,
		BytesBuffered:     1024 * 1024,
		LeasesAcquired:    15,
		LeaseConflicts:    5,
	}
	
	// Test error rate calculation
	rs := &RedisStore{stats: stats}
	errorRate := rs.calculateErrorRate()
	
	// Total ops: 100 + 50 + 20 + 30 + 40 = 240
	// Total errors: 5 + 2 + 1 + 0 + 1 = 9
	// Error rate: 9/240 = 0.0375
	expectedErrorRate := 9.0 / 240.0
	assert.InDelta(t, expectedErrorRate, errorRate, 0.001)
	
	t.Logf("Error rate calculation: %.4f (9 errors out of 240 operations)", errorRate)
}

func TestRedisStoreInterface(t *testing.T) {
	// Verify that RedisStore implements the TraceBufferStore interface
	var _ store.TraceBufferStore = (*RedisStore)(nil)
	
	// This test ensures that our RedisStore struct properly implements
	// all required methods of the TraceBufferStore interface
	t.Log("RedisStore properly implements TraceBufferStore interface")
}

func TestGenerateOwnerID(t *testing.T) {
	ownerID, err := generateOwnerID()
	require.NoError(t, err)
	assert.NotEmpty(t, ownerID)
	
	// Owner ID should contain hostname, pid, and timestamp
	assert.Contains(t, ownerID, "-")
	
	// Generate another one to ensure they're different (due to timestamp)
	time.Sleep(2 * time.Second) // Ensure different timestamp (Unix seconds)
	ownerID2, err := generateOwnerID()
	require.NoError(t, err)
	assert.NotEqual(t, ownerID, ownerID2)
	
	t.Logf("Generated owner IDs: %s, %s", ownerID, ownerID2)
}