// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryTriggerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  MemoryTriggerConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: MemoryTriggerConfig{
				RedisMemoryWarningPercent:   70.0,
				RedisMemoryCriticalPercent:  85.0,
				RedisMemoryEmergencyPercent: 95.0,
				ProcessMemoryWarningPercent:   80.0,
				ProcessMemoryCriticalPercent:  90.0,
				ProcessMemoryEmergencyPercent: 95.0,
				MaxActiveTraces:      10000,
				MaxTotalBytes:        100 * 1024 * 1024,
				MaxTraceBytes:        10 * 1024 * 1024,
				MaxTraceAge:          10 * time.Minute,
				EvaluationInterval:   30 * time.Second,
				CooldownPeriod:       2 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "invalid redis thresholds",
			config: MemoryTriggerConfig{
				RedisMemoryWarningPercent:   85.0,
				RedisMemoryCriticalPercent:  70.0, // Less than warning
				RedisMemoryEmergencyPercent: 95.0,
				ProcessMemoryWarningPercent:   80.0,
				ProcessMemoryCriticalPercent:  90.0,
				ProcessMemoryEmergencyPercent: 95.0,
				MaxActiveTraces:      10000,
				MaxTotalBytes:        100 * 1024 * 1024,
				MaxTraceBytes:        10 * 1024 * 1024,
				MaxTraceAge:          10 * time.Minute,
				EvaluationInterval:   30 * time.Second,
				CooldownPeriod:       2 * time.Minute,
			},
			wantErr: true,
			errMsg:  "redis_memory_critical_percent must be greater than warning",
		},
		{
			name: "invalid process thresholds",
			config: MemoryTriggerConfig{
				RedisMemoryWarningPercent:   70.0,
				RedisMemoryCriticalPercent:  85.0,
				RedisMemoryEmergencyPercent: 95.0,
				ProcessMemoryWarningPercent:   90.0,
				ProcessMemoryCriticalPercent:  80.0, // Less than warning
				ProcessMemoryEmergencyPercent: 95.0,
				MaxActiveTraces:      10000,
				MaxTotalBytes:        100 * 1024 * 1024,
				MaxTraceBytes:        10 * 1024 * 1024,
				MaxTraceAge:          10 * time.Minute,
				EvaluationInterval:   30 * time.Second,
				CooldownPeriod:       2 * time.Minute,
			},
			wantErr: true,
			errMsg:  "process_memory_critical_percent must be greater than warning",
		},
		{
			name: "negative max active traces",
			config: MemoryTriggerConfig{
				RedisMemoryWarningPercent:   70.0,
				RedisMemoryCriticalPercent:  85.0,
				RedisMemoryEmergencyPercent: 95.0,
				ProcessMemoryWarningPercent:   80.0,
				ProcessMemoryCriticalPercent:  90.0,
				ProcessMemoryEmergencyPercent: 95.0,
				MaxActiveTraces:      -1,
				MaxTotalBytes:        100 * 1024 * 1024,
				MaxTraceBytes:        10 * 1024 * 1024,
				MaxTraceAge:          10 * time.Minute,
				EvaluationInterval:   30 * time.Second,
				CooldownPeriod:       2 * time.Minute,
			},
			wantErr: true,
			errMsg:  "max_active_traces must be positive",
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

func TestMemoryTriggerConfig_SetDefaults(t *testing.T) {
	config := MemoryTriggerConfig{}
	config.SetDefaults()

	assert.Equal(t, 70.0, config.RedisMemoryWarningPercent)
	assert.Equal(t, 85.0, config.RedisMemoryCriticalPercent)
	assert.Equal(t, 95.0, config.RedisMemoryEmergencyPercent)
	assert.Equal(t, 80.0, config.ProcessMemoryWarningPercent)
	assert.Equal(t, 90.0, config.ProcessMemoryCriticalPercent)
	assert.Equal(t, 95.0, config.ProcessMemoryEmergencyPercent)
	assert.Equal(t, 10000, config.MaxActiveTraces)
	assert.Equal(t, int64(100*1024*1024), config.MaxTotalBytes)
	assert.Equal(t, int64(10*1024*1024), config.MaxTraceBytes)
	assert.Equal(t, 10*time.Minute, config.MaxTraceAge)
	assert.Equal(t, 30*time.Second, config.EvaluationInterval)
	assert.Equal(t, 2*time.Minute, config.CooldownPeriod)
}

func TestNewMemoryTrigger(t *testing.T) {
	tests := []struct {
		name    string
		config  MemoryTriggerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: MemoryTriggerConfig{
				Enabled: true,
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: MemoryTriggerConfig{
				Enabled:                   true,
				RedisMemoryWarningPercent: 150.0, // Invalid percentage
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger, err := NewMemoryTrigger(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, trigger)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, trigger)
				assert.Equal(t, tt.config.Enabled, trigger.IsEnabled())
			}
		})
	}
}

func TestMemoryTrigger_EvaluateRedisMemory(t *testing.T) {
	tests := []struct {
		name           string
		metrics        SystemMetrics
		expectedEvent  bool
		expectedSeverity TriggerSeverity
	}{
		{
			name: "no redis memory limit",
			metrics: SystemMetrics{
				RedisMemoryUsed: 1000,
				RedisMemoryMax:  0, // No limit
			},
			expectedEvent: false,
		},
		{
			name: "below warning threshold",
			metrics: SystemMetrics{
				RedisMemoryUsed: 60,
				RedisMemoryMax:  100,
			},
			expectedEvent: false,
		},
		{
			name: "warning threshold exceeded",
			metrics: SystemMetrics{
				RedisMemoryUsed: 75,
				RedisMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityMedium,
		},
		{
			name: "critical threshold exceeded",
			metrics: SystemMetrics{
				RedisMemoryUsed: 90,
				RedisMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityHigh,
		},
		{
			name: "emergency threshold exceeded",
			metrics: SystemMetrics{
				RedisMemoryUsed: 98,
				RedisMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid cooldown interference
			trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
				Enabled:                     true,
				RedisMemoryWarningPercent:  70.0,
				RedisMemoryCriticalPercent: 85.0,
				RedisMemoryEmergencyPercent: 95.0,
				CooldownPeriod:             0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedEvent {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventMemoryPressure, result.Event)
				assert.Equal(t, tt.expectedSeverity, result.Severity)
				assert.NotEmpty(t, result.Reason)
				assert.NotZero(t, result.TriggeredAt)
				assert.NotEmpty(t, result.Metrics)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestMemoryTrigger_EvaluateProcessMemory(t *testing.T) {
	tests := []struct {
		name           string
		metrics        SystemMetrics
		expectedEvent  bool
		expectedSeverity TriggerSeverity
	}{
		{
			name: "no process memory limit",
			metrics: SystemMetrics{
				ProcessMemoryUsed: 1000,
				ProcessMemoryMax:  0, // No limit
			},
			expectedEvent: false,
		},
		{
			name: "below warning threshold",
			metrics: SystemMetrics{
				ProcessMemoryUsed: 70,
				ProcessMemoryMax:  100,
			},
			expectedEvent: false,
		},
		{
			name: "warning threshold exceeded",
			metrics: SystemMetrics{
				ProcessMemoryUsed: 85,
				ProcessMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityMedium,
		},
		{
			name: "critical threshold exceeded",
			metrics: SystemMetrics{
				ProcessMemoryUsed: 92,
				ProcessMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityHigh,
		},
		{
			name: "emergency threshold exceeded",
			metrics: SystemMetrics{
				ProcessMemoryUsed: 97,
				ProcessMemoryMax:  100,
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid cooldown interference
			trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
				Enabled:                       true,
				ProcessMemoryWarningPercent:  80.0,
				ProcessMemoryCriticalPercent: 90.0,
				ProcessMemoryEmergencyPercent: 95.0,
				CooldownPeriod:               0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedEvent {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventMemoryPressure, result.Event)
				assert.Equal(t, tt.expectedSeverity, result.Severity)
				assert.Contains(t, result.Reason, "Process memory")
				assert.NotZero(t, result.TriggeredAt)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestMemoryTrigger_EvaluateTraceBuffer(t *testing.T) {
	tests := []struct {
		name          string
		metrics       SystemMetrics
		expectedEvent bool
	}{
		{
			name: "all limits within bounds",
			metrics: SystemMetrics{
				ActiveTraces:   50,
				TotalBytes:     500,
				OldestTraceAge: 2 * time.Minute,
			},
			expectedEvent: false,
		},
		{
			name: "active trace limit exceeded",
			metrics: SystemMetrics{
				ActiveTraces:   150,
				TotalBytes:     500,
				OldestTraceAge: 2 * time.Minute,
			},
			expectedEvent: true,
		},
		{
			name: "total bytes limit exceeded",
			metrics: SystemMetrics{
				ActiveTraces:   50,
				TotalBytes:     1500,
				OldestTraceAge: 2 * time.Minute,
			},
			expectedEvent: true,
		},
		{
			name: "trace age limit exceeded",
			metrics: SystemMetrics{
				ActiveTraces:   50,
				TotalBytes:     500,
				OldestTraceAge: 10 * time.Minute,
			},
			expectedEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid cooldown interference
			trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
				Enabled:         true,
				MaxActiveTraces: 100,
				MaxTotalBytes:   1000,
				MaxTraceAge:     5 * time.Minute,
				CooldownPeriod:  0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedEvent {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventMemoryPressure, result.Event)
				assert.NotEmpty(t, result.Reason)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestMemoryTrigger_EvaluateTraceLimits(t *testing.T) {
	traceID1 := TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	traceID2 := TraceID{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

	tests := []struct {
		name          string
		metrics       SystemMetrics
		expectedEvent bool
		expectedSeverity TriggerSeverity
	}{
		{
			name: "no oversized traces",
			metrics: SystemMetrics{
				TraceMetrics: map[string]TraceMetrics{
					traceID1.Hex(): {
						TraceID:   traceID1,
						BytesUsed: 500,
					},
					traceID2.Hex(): {
						TraceID:   traceID2,
						BytesUsed: 800,
					},
				},
			},
			expectedEvent: false,
		},
		{
			name: "single oversized trace",
			metrics: SystemMetrics{
				TraceMetrics: map[string]TraceMetrics{
					traceID1.Hex(): {
						TraceID:   traceID1,
						BytesUsed: 1500,
					},
				},
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityMedium,
		},
		{
			name: "many oversized traces",
			metrics: SystemMetrics{
				TraceMetrics: func() map[string]TraceMetrics {
					tm := make(map[string]TraceMetrics)
					for i := 0; i < 15; i++ {
						id := TraceID{byte(i)}
						tm[id.Hex()] = TraceMetrics{
							TraceID:   id,
							BytesUsed: 2000,
						}
					}
					return tm
				}(),
			},
			expectedEvent:    true,
			expectedSeverity: TriggerSeverityHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid cooldown interference
			trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
				Enabled:       true,
				MaxTraceBytes: 1000,
				CooldownPeriod: 0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedEvent {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventSizeThreshold, result.Event)
				assert.Equal(t, tt.expectedSeverity, result.Severity)
				assert.Contains(t, result.Reason, "oversized traces")
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestMemoryTrigger_Cooldown(t *testing.T) {
	trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
		Enabled:                    true,
		RedisMemoryWarningPercent: 70.0,
		CooldownPeriod:            100 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{
		RedisMemoryUsed: 80,
		RedisMemoryMax:  100,
	}

	// First evaluation should trigger
	result1, err := trigger.Evaluate(ctx, metrics)
	require.NoError(t, err)
	require.NotNil(t, result1)

	// Immediate second evaluation should not trigger (cooldown)
	result2, err := trigger.Evaluate(ctx, metrics)
	require.NoError(t, err)
	assert.Nil(t, result2)

	// Wait for cooldown to expire
	time.Sleep(150 * time.Millisecond)

	// Third evaluation should trigger again
	result3, err := trigger.Evaluate(ctx, metrics)
	require.NoError(t, err)
	require.NotNil(t, result3)
}

func TestMemoryTrigger_DisabledAndClosed(t *testing.T) {
	t.Run("disabled trigger", func(t *testing.T) {
		trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
			Enabled: false,
		})
		require.NoError(t, err)

		ctx := context.Background()
		metrics := SystemMetrics{
			RedisMemoryUsed: 99,
			RedisMemoryMax:  100,
		}

		result, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
		assert.Nil(t, result)
		assert.False(t, trigger.IsEnabled())
	})

	t.Run("closed trigger", func(t *testing.T) {
		trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
			Enabled: true,
		})
		require.NoError(t, err)

		// Close the trigger
		err = trigger.Close()
		require.NoError(t, err)
		assert.False(t, trigger.IsEnabled())

		ctx := context.Background()
		metrics := SystemMetrics{
			RedisMemoryUsed: 99,
			RedisMemoryMax:  100,
		}

		result, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestMemoryTrigger_GetType(t *testing.T) {
	trigger, err := NewMemoryTrigger(MemoryTriggerConfig{
		Enabled: true,
	})
	require.NoError(t, err)

	assert.Equal(t, TriggerEventMemoryPressure, trigger.GetType())
}