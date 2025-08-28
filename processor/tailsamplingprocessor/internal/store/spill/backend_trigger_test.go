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

func TestBackendTriggerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  BackendTriggerConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: BackendTriggerConfig{
				LatencyWarningMs:     5000,
				LatencyCriticalMs:    10000,
				LatencyEmergencyMs:   30000,
				ErrorRateWarning:     0.05,
				ErrorRateCritical:    0.10,
				ErrorRateEmergency:   0.25,
				SuccessRateWarning:   0.95,
				SuccessRateCritical:  0.90,
				SuccessRateEmergency: 0.75,
				ConsecutiveFailures:  3,
				EvaluationWindow:     5 * time.Minute,
				CooldownPeriod:       2 * time.Minute,
				RecoveryThreshold:    0.98,
				RecoveryDuration:     1 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "invalid latency thresholds",
			config: BackendTriggerConfig{
				LatencyWarningMs:     10000,
				LatencyCriticalMs:    5000, // Less than warning
				LatencyEmergencyMs:   30000,
				ErrorRateWarning:     0.05,
				ErrorRateCritical:    0.10,
				ErrorRateEmergency:   0.25,
				SuccessRateWarning:   0.95,
				SuccessRateCritical:  0.90,
				SuccessRateEmergency: 0.75,
				ConsecutiveFailures:  3,
				EvaluationWindow:     5 * time.Minute,
				CooldownPeriod:       2 * time.Minute,
				RecoveryThreshold:    0.98,
				RecoveryDuration:     1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "latency thresholds must be positive and increasing",
		},
		{
			name: "invalid error rate thresholds",
			config: BackendTriggerConfig{
				LatencyWarningMs:     5000,
				LatencyCriticalMs:    10000,
				LatencyEmergencyMs:   30000,
				ErrorRateWarning:     0.10,
				ErrorRateCritical:    0.05, // Less than warning
				ErrorRateEmergency:   0.25,
				SuccessRateWarning:   0.95,
				SuccessRateCritical:  0.90,
				SuccessRateEmergency: 0.75,
				ConsecutiveFailures:  3,
				EvaluationWindow:     5 * time.Minute,
				CooldownPeriod:       2 * time.Minute,
				RecoveryThreshold:    0.98,
				RecoveryDuration:     1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "error_rate_critical must be >= warning",
		},
		{
			name: "invalid success rate thresholds",
			config: BackendTriggerConfig{
				LatencyWarningMs:     5000,
				LatencyCriticalMs:    10000,
				LatencyEmergencyMs:   30000,
				ErrorRateWarning:     0.05,
				ErrorRateCritical:    0.10,
				ErrorRateEmergency:   0.25,
				SuccessRateWarning:   0.90,
				SuccessRateCritical:  0.95, // Greater than warning
				SuccessRateEmergency: 0.75,
				ConsecutiveFailures:  3,
				EvaluationWindow:     5 * time.Minute,
				CooldownPeriod:       2 * time.Minute,
				RecoveryThreshold:    0.98,
				RecoveryDuration:     1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "success_rate_critical must be <= warning",
		},
		{
			name: "negative consecutive failures",
			config: BackendTriggerConfig{
				LatencyWarningMs:     5000,
				LatencyCriticalMs:    10000,
				LatencyEmergencyMs:   30000,
				ErrorRateWarning:     0.05,
				ErrorRateCritical:    0.10,
				ErrorRateEmergency:   0.25,
				SuccessRateWarning:   0.95,
				SuccessRateCritical:  0.90,
				SuccessRateEmergency: 0.75,
				ConsecutiveFailures:  -1,
				EvaluationWindow:     5 * time.Minute,
				CooldownPeriod:       2 * time.Minute,
				RecoveryThreshold:    0.98,
				RecoveryDuration:     1 * time.Minute,
			},
			wantErr: true,
			errMsg:  "consecutive_failures must be positive",
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

func TestBackendTriggerConfig_SetDefaults(t *testing.T) {
	config := BackendTriggerConfig{}
	config.SetDefaults()

	assert.Equal(t, int64(5000), config.LatencyWarningMs)
	assert.Equal(t, int64(10000), config.LatencyCriticalMs)
	assert.Equal(t, int64(30000), config.LatencyEmergencyMs)
	assert.Equal(t, 0.05, config.ErrorRateWarning)
	assert.Equal(t, 0.10, config.ErrorRateCritical)
	assert.Equal(t, 0.25, config.ErrorRateEmergency)
	assert.Equal(t, 0.95, config.SuccessRateWarning)
	assert.Equal(t, 0.90, config.SuccessRateCritical)
	assert.Equal(t, 0.75, config.SuccessRateEmergency)
	assert.Equal(t, 3, config.ConsecutiveFailures)
	assert.Equal(t, 5*time.Minute, config.EvaluationWindow)
	assert.Equal(t, 2*time.Minute, config.CooldownPeriod)
	assert.Equal(t, 0.98, config.RecoveryThreshold)
	assert.Equal(t, 1*time.Minute, config.RecoveryDuration)
}

func TestNewBackendTrigger(t *testing.T) {
	tests := []struct {
		name    string
		config  BackendTriggerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: BackendTriggerConfig{
				Enabled: true,
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: BackendTriggerConfig{
				Enabled:          true,
				LatencyWarningMs: -1, // Invalid
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger, err := NewBackendTrigger(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, trigger)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, trigger)
				assert.Equal(t, tt.config.Enabled, trigger.IsEnabled())
				assert.Equal(t, BackendHealthUnknown, trigger.GetHealthStatus())
			}
		})
	}
}

func TestBackendTrigger_EvaluateLatency(t *testing.T) {
	tests := []struct {
		name             string
		metrics          SystemMetrics
		expectedTrigger  bool
		expectedSeverity TriggerSeverity
	}{
		{
			name: "latency below warning",
			metrics: SystemMetrics{
				ExporterLatency:     3 * time.Second,
				ExporterSuccessRate: 1.0,
			},
			expectedTrigger: false,
		},
		{
			name: "latency at warning level",
			metrics: SystemMetrics{
				ExporterLatency:     6 * time.Second,
				ExporterSuccessRate: 1.0,
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityLow,
		},
		{
			name: "latency at critical level",
			metrics: SystemMetrics{
				ExporterLatency:     15 * time.Second,
				ExporterSuccessRate: 1.0,
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityMedium,
		},
		{
			name: "latency at emergency level",
			metrics: SystemMetrics{
				ExporterLatency:     35 * time.Second,
				ExporterSuccessRate: 1.0,
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid state interference
			trigger, err := NewBackendTrigger(BackendTriggerConfig{
				Enabled:              true,
				LatencyWarningMs:     5000,
				LatencyCriticalMs:    10000,
				LatencyEmergencyMs:   30000,
				ConsecutiveFailures:  1,
				CooldownPeriod:       0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedTrigger {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventBackendUnavailable, result.Event)
				assert.Equal(t, tt.expectedSeverity, result.Severity)
				assert.NotEmpty(t, result.Reason)
				assert.NotZero(t, result.TriggeredAt)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestBackendTrigger_EvaluateErrorRate(t *testing.T) {
	tests := []struct {
		name             string
		metrics          SystemMetrics
		expectedTrigger  bool
		expectedSeverity TriggerSeverity
	}{
		{
			name: "error rate below warning",
			metrics: SystemMetrics{
				ExporterLatency:     1 * time.Second,
				ExporterSuccessRate: 0.98, // 2% error rate
			},
			expectedTrigger: false,
		},
		{
			name: "error rate at warning level",
			metrics: SystemMetrics{
				ExporterLatency:     1 * time.Second,
				ExporterSuccessRate: 0.93, // 7% error rate
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityLow,
		},
		{
			name: "error rate at critical level",
			metrics: SystemMetrics{
				ExporterLatency:     1 * time.Second,
				ExporterSuccessRate: 0.85, // 15% error rate
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityMedium,
		},
		{
			name: "error rate at emergency level",
			metrics: SystemMetrics{
				ExporterLatency:     1 * time.Second,
				ExporterSuccessRate: 0.70, // 30% error rate
			},
			expectedTrigger:  true,
			expectedSeverity: TriggerSeverityHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new trigger for each test to avoid state interference
			trigger, err := NewBackendTrigger(BackendTriggerConfig{
				Enabled:             true,
				ErrorRateWarning:    0.05,
				ErrorRateCritical:   0.10,
				ErrorRateEmergency:  0.25,
				ConsecutiveFailures: 1,
				CooldownPeriod:      0, // Disable cooldown for testing
			})
			require.NoError(t, err)
			
			ctx := context.Background()
			result, err := trigger.Evaluate(ctx, tt.metrics)
			require.NoError(t, err)

			if tt.expectedTrigger {
				require.NotNil(t, result)
				assert.Equal(t, TriggerEventBackendUnavailable, result.Event)
				assert.Equal(t, tt.expectedSeverity, result.Severity)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestBackendTrigger_ConsecutiveFailures(t *testing.T) {
	trigger, err := NewBackendTrigger(BackendTriggerConfig{
		Enabled:             true,
		SuccessRateWarning:  0.95,
		ConsecutiveFailures: 3,
		CooldownPeriod:      1 * time.Millisecond, // Short for testing
	})
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{
		ExporterLatency:     1 * time.Second,
		ExporterSuccessRate: 0.90, // Below warning threshold
	}

	// First two evaluations should not trigger (need 3 consecutive)
	for i := 0; i < 2; i++ {
		result, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
		assert.Nil(t, result, "evaluation %d should not trigger", i+1)
		time.Sleep(2 * time.Millisecond) // Wait for cooldown
	}

	// Third evaluation should trigger
	time.Sleep(2 * time.Millisecond)
	result, err := trigger.Evaluate(ctx, metrics)
	require.NoError(t, err)
	require.NotNil(t, result, "third evaluation should trigger")
	assert.Equal(t, TriggerEventBackendUnavailable, result.Event)
}

func TestBackendTrigger_Recovery(t *testing.T) {
	trigger, err := NewBackendTrigger(BackendTriggerConfig{
		Enabled:             true,
		SuccessRateWarning:  0.95,
		ConsecutiveFailures: 1,
		RecoveryThreshold:   0.98,
		RecoveryDuration:    100 * time.Millisecond,
		CooldownPeriod:      1 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// First evaluation with poor metrics - should trigger
	poorMetrics := SystemMetrics{
		ExporterLatency:     1 * time.Second,
		ExporterSuccessRate: 0.80,
	}
	result1, err := trigger.Evaluate(ctx, poorMetrics)
	require.NoError(t, err)
	require.NotNil(t, result1)

	// Wait for cooldown
	time.Sleep(2 * time.Millisecond)

	// Second evaluation with good metrics - should not trigger recovery yet
	goodMetrics := SystemMetrics{
		ExporterLatency:     1 * time.Second,
		ExporterSuccessRate: 0.99,
	}
	result2, err := trigger.Evaluate(ctx, goodMetrics)
	require.NoError(t, err)
	assert.Nil(t, result2, "should not trigger during recovery")

	// Wait for recovery duration
	time.Sleep(150 * time.Millisecond)

	// Third evaluation with good metrics - should complete recovery
	result3, err := trigger.Evaluate(ctx, goodMetrics)
	require.NoError(t, err)
	assert.Nil(t, result3, "should not trigger after recovery")

	// Verify health status
	assert.Equal(t, BackendHealthHealthy, trigger.GetHealthStatus())
}

func TestBackendTrigger_HealthHistory(t *testing.T) {
	trigger, err := NewBackendTrigger(BackendTriggerConfig{
		Enabled:          true,
		EvaluationWindow: 1 * time.Minute,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Add multiple evaluations
	for i := 0; i < 5; i++ {
		metrics := SystemMetrics{
			ExporterLatency:     time.Duration(i) * time.Second,
			ExporterSuccessRate: 0.95 - float64(i)*0.01,
			ExporterErrors:      int64(i * 10),
		}
		_, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
	}

	// Check health history
	history := trigger.GetHealthHistory()
	assert.NotEmpty(t, history)
	assert.LessOrEqual(t, len(history), 5)

	// Verify history entries have proper fields
	for _, entry := range history {
		assert.NotZero(t, entry.WindowStart)
		assert.NotZero(t, entry.WindowEnd)
		assert.True(t, entry.WindowEnd.After(entry.WindowStart))
	}
}

func TestBackendTrigger_DisabledAndClosed(t *testing.T) {
	t.Run("disabled trigger", func(t *testing.T) {
		trigger, err := NewBackendTrigger(BackendTriggerConfig{
			Enabled: false,
		})
		require.NoError(t, err)

		ctx := context.Background()
		metrics := SystemMetrics{
			ExporterLatency:     100 * time.Second,
			ExporterSuccessRate: 0.10,
		}

		result, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
		assert.Nil(t, result)
		assert.False(t, trigger.IsEnabled())
	})

	t.Run("closed trigger", func(t *testing.T) {
		trigger, err := NewBackendTrigger(BackendTriggerConfig{
			Enabled: true,
		})
		require.NoError(t, err)

		// Close the trigger
		err = trigger.Close()
		require.NoError(t, err)
		assert.False(t, trigger.IsEnabled())

		ctx := context.Background()
		metrics := SystemMetrics{
			ExporterLatency:     100 * time.Second,
			ExporterSuccessRate: 0.10,
		}

		result, err := trigger.Evaluate(ctx, metrics)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestBackendTrigger_GetType(t *testing.T) {
	trigger, err := NewBackendTrigger(BackendTriggerConfig{
		Enabled: true,
	})
	require.NoError(t, err)

	assert.Equal(t, TriggerEventBackendUnavailable, trigger.GetType())
}

func TestBackendHealthStatus_String(t *testing.T) {
	tests := []struct {
		status   BackendHealthStatus
		expected string
	}{
		{BackendHealthUnknown, "unknown"},
		{BackendHealthHealthy, "healthy"},
		{BackendHealthWarning, "warning"},
		{BackendHealthCritical, "critical"},
		{BackendHealthUnavailable, "unavailable"},
		{BackendHealthStatus(99), "unknown"}, // Invalid status
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}