// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockTrigger implements SpillTrigger for testing
type MockTrigger struct {
	triggerType TriggerEvent
	enabled     bool
	closed      bool
	triggerCtx  *TriggerContext
	evaluateErr error
}

func (mt *MockTrigger) Evaluate(ctx context.Context, metrics SystemMetrics) (*TriggerContext, error) {
	if mt.evaluateErr != nil {
		return nil, mt.evaluateErr
	}
	if !mt.enabled || mt.closed {
		return nil, nil
	}
	return mt.triggerCtx, nil
}

func (mt *MockTrigger) GetType() TriggerEvent {
	return mt.triggerType
}

func (mt *MockTrigger) IsEnabled() bool {
	return mt.enabled && !mt.closed
}

func (mt *MockTrigger) Close() error {
	mt.closed = true
	return nil
}

func TestSpillPolicyConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  SpillPolicyConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: SpillPolicyConfig{
				EvaluationInterval: 30 * time.Second,
				SelectionStrategy:  "priority",
				MaxTargetsPerSpill: 100,
				MinDeferTime:       30 * time.Second,
				MaxDeferTime:       5 * time.Minute,
				MaxSpillsPerHour:   100,
				ConsensusRatio:     0.5,
			},
			wantErr: false,
		},
		{
			name: "invalid evaluation interval",
			config: SpillPolicyConfig{
				EvaluationInterval: 0,
				SelectionStrategy:  "priority",
				MaxTargetsPerSpill: 100,
				MinDeferTime:       30 * time.Second,
				MaxDeferTime:       5 * time.Minute,
				MaxSpillsPerHour:   100,
				ConsensusRatio:     0.5,
			},
			wantErr: true,
			errMsg:  "evaluation_interval must be positive",
		},
		{
			name: "invalid selection strategy",
			config: SpillPolicyConfig{
				EvaluationInterval: 30 * time.Second,
				SelectionStrategy:  "invalid",
				MaxTargetsPerSpill: 100,
				MinDeferTime:       30 * time.Second,
				MaxDeferTime:       5 * time.Minute,
				MaxSpillsPerHour:   100,
				ConsensusRatio:     0.5,
			},
			wantErr: true,
			errMsg:  "invalid selection_strategy",
		},
		{
			name: "max defer time less than min",
			config: SpillPolicyConfig{
				EvaluationInterval: 30 * time.Second,
				SelectionStrategy:  "priority",
				MaxTargetsPerSpill: 100,
				MinDeferTime:       5 * time.Minute,
				MaxDeferTime:       30 * time.Second,
				MaxSpillsPerHour:   100,
				ConsensusRatio:     0.5,
			},
			wantErr: true,
			errMsg:  "max_defer_time must be greater than min_defer_time",
		},
		{
			name: "invalid consensus ratio",
			config: SpillPolicyConfig{
				EvaluationInterval: 30 * time.Second,
				SelectionStrategy:  "priority",
				MaxTargetsPerSpill: 100,
				MinDeferTime:       30 * time.Second,
				MaxDeferTime:       5 * time.Minute,
				MaxSpillsPerHour:   100,
				ConsensusRatio:     1.5,
			},
			wantErr: true,
			errMsg:  "consensus_ratio must be between 0 and 1",
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

func TestSpillPolicyConfig_SetDefaults(t *testing.T) {
	config := SpillPolicyConfig{}
	config.SetDefaults()

	assert.Equal(t, 30*time.Second, config.EvaluationInterval)
	assert.Equal(t, "priority", config.SelectionStrategy)
	assert.Equal(t, 100, config.MaxTargetsPerSpill)
	assert.Equal(t, 30*time.Second, config.MinDeferTime)
	assert.Equal(t, 5*time.Minute, config.MaxDeferTime)
	assert.Equal(t, 100, config.MaxSpillsPerHour)
	assert.Equal(t, 0.5, config.ConsensusRatio)
}

func TestNewDefaultSpillPolicy(t *testing.T) {
	tests := []struct {
		name    string
		config  SpillPolicyConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: SpillPolicyConfig{
				Enabled: true,
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: SpillPolicyConfig{
				Enabled:            true,
				EvaluationInterval: -1, // Invalid
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := NewDefaultSpillPolicy(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, policy)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, policy)
			}
		})
	}
}

func TestDefaultSpillPolicy_RegisterUnregisterTriggers(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled: true,
	})
	require.NoError(t, err)

	// Register a trigger
	trigger1 := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
	}
	err = policy.RegisterTrigger(trigger1)
	require.NoError(t, err)

	// Verify trigger is registered
	activeTriggers := policy.GetActiveTriggers()
	assert.Len(t, activeTriggers, 1)

	// Try to register duplicate trigger type
	trigger2 := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
	}
	err = policy.RegisterTrigger(trigger2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")

	// Register different trigger type
	trigger3 := &MockTrigger{
		triggerType: TriggerEventBackendUnavailable,
		enabled:     true,
	}
	err = policy.RegisterTrigger(trigger3)
	require.NoError(t, err)

	// Verify both triggers are registered
	activeTriggers = policy.GetActiveTriggers()
	assert.Len(t, activeTriggers, 2)

	// Unregister a trigger
	err = policy.UnregisterTrigger(TriggerEventMemoryPressure)
	require.NoError(t, err)

	// Verify trigger is removed
	activeTriggers = policy.GetActiveTriggers()
	assert.Len(t, activeTriggers, 1)

	// Try to unregister non-existent trigger
	err = policy.UnregisterTrigger(TriggerEventSizeThreshold)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDefaultSpillPolicy_EvaluateSpill_NoTriggers(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{}

	decision, err := policy.EvaluateSpill(ctx, metrics)
	require.NoError(t, err)
	assert.False(t, decision.ShouldSpill)
	assert.Equal(t, "no active triggers", decision.Reason)
	assert.NotZero(t, decision.DeferFor)
}

func TestDefaultSpillPolicy_EvaluateSpill_WithTriggers(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled:            true,
		MaxTargetsPerSpill: 2,
	})
	require.NoError(t, err)

	traceID1 := TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	traceID2 := TraceID{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

	// Register a trigger that will fire
	trigger := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
		triggerCtx: &TriggerContext{
			Event:       TriggerEventMemoryPressure,
			Severity:    TriggerSeverityHigh,
			Reason:      "test trigger",
			TriggeredAt: time.Now(),
		},
	}
	err = policy.RegisterTrigger(trigger)
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{
		TraceMetrics: map[string]TraceMetrics{
			traceID1.Hex(): {
				TraceID:   traceID1,
				BytesUsed: 1000,
				SpanCount: 10,
				FirstSeen: time.Now().Add(-5 * time.Minute),
			},
			traceID2.Hex(): {
				TraceID:   traceID2,
				BytesUsed: 2000,
				SpanCount: 20,
				FirstSeen: time.Now().Add(-10 * time.Minute),
			},
		},
	}

	decision, err := policy.EvaluateSpill(ctx, metrics)
	require.NoError(t, err)
	assert.True(t, decision.ShouldSpill)
	assert.Contains(t, decision.Reason, "memory_pressure")
	assert.Contains(t, decision.Reason, "high")
	assert.Len(t, decision.Targets, 2) // Limited by MaxTargetsPerSpill

	// Verify statistics were updated
	stats := policy.GetStats()
	assert.Equal(t, int64(1), stats.TotalEvaluations)
	assert.Equal(t, int64(1), stats.TotalSpillsTriggered)
	assert.Equal(t, int64(2), stats.TotalTargetsSpilled)
}

func TestDefaultSpillPolicy_EvaluateSpill_Consensus(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled:          true,
		RequireConsensus: true,
		ConsensusRatio:   0.5,
	})
	require.NoError(t, err)

	// Register two triggers, only one will fire
	trigger1 := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
		triggerCtx: &TriggerContext{
			Event:    TriggerEventMemoryPressure,
			Severity: TriggerSeverityHigh,
		},
	}
	trigger2 := &MockTrigger{
		triggerType: TriggerEventBackendUnavailable,
		enabled:     true,
		triggerCtx:  nil, // Won't fire
	}

	err = policy.RegisterTrigger(trigger1)
	require.NoError(t, err)
	err = policy.RegisterTrigger(trigger2)
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{}

	// With 1/2 triggers firing and consensus ratio of 0.5, should trigger
	decision, err := policy.EvaluateSpill(ctx, metrics)
	require.NoError(t, err)
	assert.True(t, decision.ShouldSpill)

	// Change consensus ratio to require both triggers
	policy.config.ConsensusRatio = 0.75

	decision, err = policy.EvaluateSpill(ctx, metrics)
	require.NoError(t, err)
	assert.False(t, decision.ShouldSpill)
	assert.Contains(t, decision.Reason, "insufficient consensus")
}

func TestDefaultSpillPolicy_SelectionStrategies(t *testing.T) {
	traceID1 := TraceID{1}
	traceID2 := TraceID{2}
	traceID3 := TraceID{3}

	baseMetrics := SystemMetrics{
		TraceMetrics: map[string]TraceMetrics{
			traceID1.Hex(): {
				TraceID:   traceID1,
				BytesUsed: 3000,
				SpanCount: 30,
				FirstSeen: time.Now().Add(-15 * time.Minute), // Priority: 3 (age)
			},
			traceID2.Hex(): {
				TraceID:   traceID2,
				BytesUsed: 1000,
				SpanCount: 10,
				FirstSeen: time.Now().Add(-3 * time.Minute), // Priority: 1 (age)
			},
			traceID3.Hex(): {
				TraceID:   traceID3,
				BytesUsed: 2000,
				SpanCount: 20,
				FirstSeen: time.Now().Add(-6 * time.Minute), // Priority: 2 (age)
			},
		},
	}

	tests := []struct {
		name             string
		strategy         string
		expectedFirstID  TraceID
		expectedSecondID TraceID
	}{
		{
			name:             "oldest strategy",
			strategy:         "oldest",
			expectedFirstID:  traceID1, // 15 minutes old
			expectedSecondID: traceID3, // 10 minutes old
		},
		{
			name:             "largest strategy",
			strategy:         "largest",
			expectedFirstID:  traceID1, // 3000 bytes
			expectedSecondID: traceID3, // 2000 bytes
		},
		{
			name:             "priority strategy",
			strategy:         "priority",
			expectedFirstID:  traceID1, // Highest priority (oldest + largest)
			expectedSecondID: traceID3, // Second priority
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
				Enabled:            true,
				SelectionStrategy:  tt.strategy,
				MaxTargetsPerSpill: 2,
			})
			require.NoError(t, err)

			// Register trigger that will fire
			trigger := &MockTrigger{
				triggerType: TriggerEventMemoryPressure,
				enabled:     true,
				triggerCtx: &TriggerContext{
					Event:    TriggerEventMemoryPressure,
					Severity: TriggerSeverityHigh,
				},
			}
			err = policy.RegisterTrigger(trigger)
			require.NoError(t, err)

			ctx := context.Background()
			decision, err := policy.EvaluateSpill(ctx, baseMetrics)
			require.NoError(t, err)
			require.True(t, decision.ShouldSpill)
			require.Len(t, decision.Targets, 2)

			assert.Equal(t, tt.expectedFirstID, decision.Targets[0].TraceID)
			assert.Equal(t, tt.expectedSecondID, decision.Targets[1].TraceID)
		})
	}
}

func TestDefaultSpillPolicy_RateLimiting(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled:          true,
		MaxSpillsPerHour: 2,
	})
	require.NoError(t, err)

	// Register trigger that will always fire
	trigger := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
		triggerCtx: &TriggerContext{
			Event:    TriggerEventMemoryPressure,
			Severity: TriggerSeverityHigh,
		},
	}
	err = policy.RegisterTrigger(trigger)
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{
		TraceMetrics: map[string]TraceMetrics{
			"trace1": {BytesUsed: 1000},
		},
	}

	// First two spills should succeed
	for i := 0; i < 2; i++ {
		decision, err := policy.EvaluateSpill(ctx, metrics)
		require.NoError(t, err)
		assert.True(t, decision.ShouldSpill, "spill %d should succeed", i+1)
	}

	// Third spill should be rate limited
	decision, err := policy.EvaluateSpill(ctx, metrics)
	require.NoError(t, err)
	assert.False(t, decision.ShouldSpill)
	assert.Contains(t, decision.Reason, "rate limited")
}

func TestDefaultSpillPolicy_TriggerError(t *testing.T) {
	policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
		Enabled: true,
	})
	require.NoError(t, err)

	// Register trigger that will return error
	trigger := &MockTrigger{
		triggerType: TriggerEventMemoryPressure,
		enabled:     true,
		evaluateErr: fmt.Errorf("trigger error"),
	}
	err = policy.RegisterTrigger(trigger)
	require.NoError(t, err)

	ctx := context.Background()
	metrics := SystemMetrics{}

	decision, err := policy.EvaluateSpill(ctx, metrics)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trigger error")
	assert.False(t, decision.ShouldSpill)
}

func TestDefaultSpillPolicy_DisabledAndClosed(t *testing.T) {
	t.Run("disabled policy", func(t *testing.T) {
		policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
			Enabled: false,
		})
		require.NoError(t, err)

		// Register trigger
		trigger := &MockTrigger{
			triggerType: TriggerEventMemoryPressure,
			enabled:     true,
			triggerCtx: &TriggerContext{
				Event:    TriggerEventMemoryPressure,
				Severity: TriggerSeverityHigh,
			},
		}
		err = policy.RegisterTrigger(trigger)
		require.NoError(t, err)

		ctx := context.Background()
		metrics := SystemMetrics{}

		decision, err := policy.EvaluateSpill(ctx, metrics)
		require.NoError(t, err)
		assert.False(t, decision.ShouldSpill)
		assert.Equal(t, "spill policy disabled", decision.Reason)
	})

	t.Run("closed policy", func(t *testing.T) {
		policy, err := NewDefaultSpillPolicy(SpillPolicyConfig{
			Enabled: true,
		})
		require.NoError(t, err)

		// Register trigger
		trigger := &MockTrigger{
			triggerType: TriggerEventMemoryPressure,
			enabled:     true,
		}
		err = policy.RegisterTrigger(trigger)
		require.NoError(t, err)

		// Close the policy
		err = policy.Close()
		require.NoError(t, err)

		// Verify trigger was closed
		assert.True(t, trigger.closed)

		// Try to register after close
		trigger2 := &MockTrigger{
			triggerType: TriggerEventBackendUnavailable,
			enabled:     true,
		}
		err = policy.RegisterTrigger(trigger2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "policy is closed")
	})
}

func TestTriggerEvent_String(t *testing.T) {
	tests := []struct {
		event    TriggerEvent
		expected string
	}{
		{TriggerEventMemoryPressure, "memory_pressure"},
		{TriggerEventSizeThreshold, "size_threshold"},
		{TriggerEventAgeThreshold, "age_threshold"},
		{TriggerEventBackendUnavailable, "backend_unavailable"},
		{TriggerEventSystemPressure, "system_pressure"},
		{TriggerEventManualTrigger, "manual_trigger"},
		{TriggerEvent(99), "unknown"}, // Invalid event
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.event.String())
		})
	}
}

func TestTriggerSeverity_String(t *testing.T) {
	tests := []struct {
		severity TriggerSeverity
		expected string
	}{
		{TriggerSeverityLow, "low"},
		{TriggerSeverityMedium, "medium"},
		{TriggerSeverityHigh, "high"},
		{TriggerSeverityCritical, "critical"},
		{TriggerSeverity(99), "unknown"}, // Invalid severity
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.severity.String())
		})
	}
}