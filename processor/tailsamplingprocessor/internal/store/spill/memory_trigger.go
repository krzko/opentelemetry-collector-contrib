// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryTriggerConfig configures the memory pressure trigger
type MemoryTriggerConfig struct {
	// Enabled determines if this trigger is active
	Enabled bool `json:"enabled" yaml:"enabled"`
	
	// Redis memory thresholds
	RedisMemoryWarningPercent   float64 `json:"redis_memory_warning_percent" yaml:"redis_memory_warning_percent"`     // Default: 70.0
	RedisMemoryCriticalPercent  float64 `json:"redis_memory_critical_percent" yaml:"redis_memory_critical_percent"`   // Default: 85.0
	RedisMemoryEmergencyPercent float64 `json:"redis_memory_emergency_percent" yaml:"redis_memory_emergency_percent"` // Default: 95.0
	
	// Process memory thresholds  
	ProcessMemoryWarningPercent   float64 `json:"process_memory_warning_percent" yaml:"process_memory_warning_percent"`     // Default: 80.0
	ProcessMemoryCriticalPercent  float64 `json:"process_memory_critical_percent" yaml:"process_memory_critical_percent"`   // Default: 90.0
	ProcessMemoryEmergencyPercent float64 `json:"process_memory_emergency_percent" yaml:"process_memory_emergency_percent"` // Default: 95.0
	
	// Trace buffer thresholds
	MaxActiveTraces      int   `json:"max_active_traces" yaml:"max_active_traces"`           // Default: 10000
	MaxTotalBytes        int64 `json:"max_total_bytes" yaml:"max_total_bytes"`               // Default: 100MB
	MaxTraceBytes        int64 `json:"max_trace_bytes" yaml:"max_trace_bytes"`               // Default: 10MB
	MaxTraceAge          time.Duration `json:"max_trace_age" yaml:"max_trace_age"`           // Default: 10m
	
	// Evaluation settings
	EvaluationInterval   time.Duration `json:"evaluation_interval" yaml:"evaluation_interval"` // Default: 30s
	CooldownPeriod       time.Duration `json:"cooldown_period" yaml:"cooldown_period"`         // Default: 2m
}

// Validate checks the memory trigger configuration
func (c *MemoryTriggerConfig) Validate() error {
	if c.RedisMemoryWarningPercent <= 0 || c.RedisMemoryWarningPercent > 100 {
		return fmt.Errorf("redis_memory_warning_percent must be between 0 and 100")
	}
	if c.RedisMemoryCriticalPercent <= c.RedisMemoryWarningPercent || c.RedisMemoryCriticalPercent > 100 {
		return fmt.Errorf("redis_memory_critical_percent must be greater than warning and <= 100")
	}
	if c.RedisMemoryEmergencyPercent <= c.RedisMemoryCriticalPercent || c.RedisMemoryEmergencyPercent > 100 {
		return fmt.Errorf("redis_memory_emergency_percent must be greater than critical and <= 100")
	}
	
	if c.ProcessMemoryWarningPercent <= 0 || c.ProcessMemoryWarningPercent > 100 {
		return fmt.Errorf("process_memory_warning_percent must be between 0 and 100")
	}
	if c.ProcessMemoryCriticalPercent <= c.ProcessMemoryWarningPercent || c.ProcessMemoryCriticalPercent > 100 {
		return fmt.Errorf("process_memory_critical_percent must be greater than warning and <= 100")
	}
	if c.ProcessMemoryEmergencyPercent <= c.ProcessMemoryCriticalPercent || c.ProcessMemoryEmergencyPercent > 100 {
		return fmt.Errorf("process_memory_emergency_percent must be greater than critical and <= 100")
	}
	
	if c.MaxActiveTraces <= 0 {
		return fmt.Errorf("max_active_traces must be positive")
	}
	if c.MaxTotalBytes <= 0 {
		return fmt.Errorf("max_total_bytes must be positive")
	}
	if c.MaxTraceBytes <= 0 {
		return fmt.Errorf("max_trace_bytes must be positive")
	}
	if c.MaxTraceAge <= 0 {
		return fmt.Errorf("max_trace_age must be positive")
	}
	if c.EvaluationInterval <= 0 {
		return fmt.Errorf("evaluation_interval must be positive")
	}
	if c.CooldownPeriod <= 0 {
		return fmt.Errorf("cooldown_period must be positive")
	}
	
	return nil
}

// SetDefaults sets default values for the memory trigger configuration
func (c *MemoryTriggerConfig) SetDefaults() {
	if c.RedisMemoryWarningPercent == 0 {
		c.RedisMemoryWarningPercent = 70.0
	}
	if c.RedisMemoryCriticalPercent == 0 {
		c.RedisMemoryCriticalPercent = 85.0
	}
	if c.RedisMemoryEmergencyPercent == 0 {
		c.RedisMemoryEmergencyPercent = 95.0
	}
	
	if c.ProcessMemoryWarningPercent == 0 {
		c.ProcessMemoryWarningPercent = 80.0
	}
	if c.ProcessMemoryCriticalPercent == 0 {
		c.ProcessMemoryCriticalPercent = 90.0
	}
	if c.ProcessMemoryEmergencyPercent == 0 {
		c.ProcessMemoryEmergencyPercent = 95.0
	}
	
	if c.MaxActiveTraces == 0 {
		c.MaxActiveTraces = 10000
	}
	if c.MaxTotalBytes == 0 {
		c.MaxTotalBytes = 100 * 1024 * 1024 // 100MB
	}
	if c.MaxTraceBytes == 0 {
		c.MaxTraceBytes = 10 * 1024 * 1024 // 10MB
	}
	if c.MaxTraceAge == 0 {
		c.MaxTraceAge = 10 * time.Minute
	}
	if c.EvaluationInterval == 0 {
		c.EvaluationInterval = 30 * time.Second
	}
	if c.CooldownPeriod == 0 {
		c.CooldownPeriod = 2 * time.Minute
	}
}

// MemoryTrigger monitors memory pressure and triggers spill operations when thresholds are exceeded.
// Implements the SpillTrigger interface following the Single Responsibility Principle.
type MemoryTrigger struct {
	config       MemoryTriggerConfig
	lastTrigger  time.Time
	mu           sync.RWMutex
	closed       bool
}

// NewMemoryTrigger creates a new memory pressure trigger
func NewMemoryTrigger(config MemoryTriggerConfig) (*MemoryTrigger, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid memory trigger config: %w", err)
	}
	
	return &MemoryTrigger{
		config: config,
	}, nil
}

// Evaluate assesses current memory conditions and determines if spill should be triggered
func (mt *MemoryTrigger) Evaluate(ctx context.Context, metrics SystemMetrics) (*TriggerContext, error) {
	mt.mu.RLock()
	closed := mt.closed
	enabled := mt.config.Enabled
	lastTrigger := mt.lastTrigger
	cooldownPeriod := mt.config.CooldownPeriod
	mt.mu.RUnlock()
	
	if closed || !enabled {
		return nil, nil
	}
	
	// Check cooldown period
	if time.Since(lastTrigger) < cooldownPeriod {
		return nil, nil
	}
	
	// Evaluate Redis memory pressure
	if trigger := mt.evaluateRedisMemory(metrics); trigger != nil {
		mt.updateLastTrigger()
		return trigger, nil
	}
	
	// Evaluate process memory pressure
	if trigger := mt.evaluateProcessMemory(metrics); trigger != nil {
		mt.updateLastTrigger()
		return trigger, nil
	}
	
	// Evaluate trace buffer limits
	if trigger := mt.evaluateTraceBuffer(metrics); trigger != nil {
		mt.updateLastTrigger()
		return trigger, nil
	}
	
	// Evaluate individual trace limits
	if trigger := mt.evaluateTraceLimits(metrics); trigger != nil {
		mt.updateLastTrigger()
		return trigger, nil
	}
	
	return nil, nil
}

// evaluateRedisMemory checks Redis memory usage against thresholds
func (mt *MemoryTrigger) evaluateRedisMemory(metrics SystemMetrics) *TriggerContext {
	if metrics.RedisMemoryMax == 0 {
		return nil // No Redis memory limit configured
	}
	
	usagePercent := float64(metrics.RedisMemoryUsed) / float64(metrics.RedisMemoryMax) * 100
	
	var severity TriggerSeverity
	var reason string
	
	if usagePercent >= mt.config.RedisMemoryEmergencyPercent {
		severity = TriggerSeverityCritical
		reason = fmt.Sprintf("Redis memory usage at emergency level: %.1f%% (emergency threshold: %.1f%%)", 
			usagePercent, mt.config.RedisMemoryEmergencyPercent)
	} else if usagePercent >= mt.config.RedisMemoryCriticalPercent {
		severity = TriggerSeverityHigh
		reason = fmt.Sprintf("Redis memory usage at critical level: %.1f%% (critical threshold: %.1f%%)", 
			usagePercent, mt.config.RedisMemoryCriticalPercent)
	} else if usagePercent >= mt.config.RedisMemoryWarningPercent {
		severity = TriggerSeverityMedium
		reason = fmt.Sprintf("Redis memory usage at warning level: %.1f%% (warning threshold: %.1f%%)", 
			usagePercent, mt.config.RedisMemoryWarningPercent)
	} else {
		return nil // No threshold exceeded
	}
	
	return &TriggerContext{
		Event:       TriggerEventMemoryPressure,
		Severity:    severity,
		Reason:      reason,
		TriggeredAt: time.Now(),
		Metrics: map[string]float64{
			"redis_memory_used_bytes":    float64(metrics.RedisMemoryUsed),
			"redis_memory_max_bytes":     float64(metrics.RedisMemoryMax),
			"redis_memory_usage_percent": usagePercent,
		},
	}
}

// evaluateProcessMemory checks process memory usage against thresholds
func (mt *MemoryTrigger) evaluateProcessMemory(metrics SystemMetrics) *TriggerContext {
	if metrics.ProcessMemoryMax == 0 {
		return nil // No process memory limit configured
	}
	
	usagePercent := float64(metrics.ProcessMemoryUsed) / float64(metrics.ProcessMemoryMax) * 100
	
	var severity TriggerSeverity
	var reason string
	
	if usagePercent >= mt.config.ProcessMemoryEmergencyPercent {
		severity = TriggerSeverityCritical
		reason = fmt.Sprintf("Process memory usage at emergency level: %.1f%% (emergency threshold: %.1f%%)", 
			usagePercent, mt.config.ProcessMemoryEmergencyPercent)
	} else if usagePercent >= mt.config.ProcessMemoryCriticalPercent {
		severity = TriggerSeverityHigh
		reason = fmt.Sprintf("Process memory usage at critical level: %.1f%% (critical threshold: %.1f%%)", 
			usagePercent, mt.config.ProcessMemoryCriticalPercent)
	} else if usagePercent >= mt.config.ProcessMemoryWarningPercent {
		severity = TriggerSeverityMedium
		reason = fmt.Sprintf("Process memory usage at warning level: %.1f%% (warning threshold: %.1f%%)", 
			usagePercent, mt.config.ProcessMemoryWarningPercent)
	} else {
		return nil // No threshold exceeded
	}
	
	return &TriggerContext{
		Event:       TriggerEventMemoryPressure,
		Severity:    severity,
		Reason:      reason,
		TriggeredAt: time.Now(),
		Metrics: map[string]float64{
			"process_memory_used_bytes":    float64(metrics.ProcessMemoryUsed),
			"process_memory_max_bytes":     float64(metrics.ProcessMemoryMax),
			"process_memory_usage_percent": usagePercent,
		},
	}
}

// evaluateTraceBuffer checks trace buffer limits
func (mt *MemoryTrigger) evaluateTraceBuffer(metrics SystemMetrics) *TriggerContext {
	var severity TriggerSeverity
	var reason string
	
	// Check active trace count
	if metrics.ActiveTraces >= mt.config.MaxActiveTraces {
		severity = TriggerSeverityHigh
		reason = fmt.Sprintf("Active trace count limit exceeded: %d (limit: %d)", 
			metrics.ActiveTraces, mt.config.MaxActiveTraces)
	}
	
	// Check total memory usage
	if metrics.TotalBytes >= mt.config.MaxTotalBytes {
		newSeverity := TriggerSeverityHigh
		newReason := fmt.Sprintf("Total trace buffer memory limit exceeded: %d bytes (limit: %d bytes)", 
			metrics.TotalBytes, mt.config.MaxTotalBytes)
		
		// Use the higher severity if multiple limits are exceeded
		if severity == 0 || newSeverity > severity {
			severity = newSeverity
			reason = newReason
		}
	}
	
	// Check oldest trace age
	if metrics.OldestTraceAge >= mt.config.MaxTraceAge {
		newSeverity := TriggerSeverityMedium
		newReason := fmt.Sprintf("Oldest trace age limit exceeded: %v (limit: %v)", 
			metrics.OldestTraceAge, mt.config.MaxTraceAge)
		
		// Use the higher severity if multiple limits are exceeded
		if severity == 0 || newSeverity > severity {
			severity = newSeverity
			reason = newReason
		}
	}
	
	if severity == 0 {
		return nil // No threshold exceeded
	}
	
	return &TriggerContext{
		Event:       TriggerEventMemoryPressure,
		Severity:    severity,
		Reason:      reason,
		TriggeredAt: time.Now(),
		Metrics: map[string]float64{
			"active_traces":       float64(metrics.ActiveTraces),
			"total_bytes":         float64(metrics.TotalBytes),
			"oldest_trace_age_ms": float64(metrics.OldestTraceAge.Milliseconds()),
		},
	}
}

// evaluateTraceLimits checks individual trace size limits
func (mt *MemoryTrigger) evaluateTraceLimits(metrics SystemMetrics) *TriggerContext {
	oversizedTraces := 0
	maxOversize := int64(0)
	var worstTraceID TraceID
	
	for _, traceMetrics := range metrics.TraceMetrics {
		if traceMetrics.BytesUsed >= mt.config.MaxTraceBytes {
			oversizedTraces++
			if traceMetrics.BytesUsed > maxOversize {
				maxOversize = traceMetrics.BytesUsed
				worstTraceID = traceMetrics.TraceID
			}
		}
	}
	
	if oversizedTraces == 0 {
		return nil // No oversized traces
	}
	
	severity := TriggerSeverityMedium
	if oversizedTraces > 10 {
		severity = TriggerSeverityHigh
	}
	
	reason := fmt.Sprintf("Found %d oversized traces (limit: %d bytes), largest: %d bytes", 
		oversizedTraces, mt.config.MaxTraceBytes, maxOversize)
	
	return &TriggerContext{
		Event:       TriggerEventSizeThreshold,
		Severity:    severity,
		TraceID:     worstTraceID,
		Reason:      reason,
		TriggeredAt: time.Now(),
		Metrics: map[string]float64{
			"oversized_traces":    float64(oversizedTraces),
			"max_trace_bytes":     float64(mt.config.MaxTraceBytes),
			"largest_trace_bytes": float64(maxOversize),
		},
	}
}

// updateLastTrigger updates the last trigger time
func (mt *MemoryTrigger) updateLastTrigger() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.lastTrigger = time.Now()
}

// GetType returns the type of events this trigger monitors
func (mt *MemoryTrigger) GetType() TriggerEvent {
	return TriggerEventMemoryPressure
}

// IsEnabled returns true if this trigger is currently active
func (mt *MemoryTrigger) IsEnabled() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.config.Enabled && !mt.closed
}

// Close releases any resources held by the trigger
func (mt *MemoryTrigger) Close() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.closed = true
	return nil
}