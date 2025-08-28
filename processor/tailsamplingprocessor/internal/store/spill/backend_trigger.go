// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BackendTriggerConfig configures the backend unavailability trigger
type BackendTriggerConfig struct {
	// Enabled determines if this trigger is active
	Enabled bool `json:"enabled" yaml:"enabled"`
	
	// Exporter health thresholds
	LatencyWarningMs     int64   `json:"latency_warning_ms" yaml:"latency_warning_ms"`         // Default: 5000ms
	LatencyCriticalMs    int64   `json:"latency_critical_ms" yaml:"latency_critical_ms"`       // Default: 10000ms
	LatencyEmergencyMs   int64   `json:"latency_emergency_ms" yaml:"latency_emergency_ms"`     // Default: 30000ms
	
	ErrorRateWarning     float64 `json:"error_rate_warning" yaml:"error_rate_warning"`         // Default: 0.05 (5%)
	ErrorRateCritical    float64 `json:"error_rate_critical" yaml:"error_rate_critical"`       // Default: 0.10 (10%)
	ErrorRateEmergency   float64 `json:"error_rate_emergency" yaml:"error_rate_emergency"`     // Default: 0.25 (25%)
	
	SuccessRateWarning   float64 `json:"success_rate_warning" yaml:"success_rate_warning"`     // Default: 0.95 (95%)
	SuccessRateCritical  float64 `json:"success_rate_critical" yaml:"success_rate_critical"`   // Default: 0.90 (90%)
	SuccessRateEmergency float64 `json:"success_rate_emergency" yaml:"success_rate_emergency"` // Default: 0.75 (75%)
	
	// Trigger behavior
	ConsecutiveFailures  int           `json:"consecutive_failures" yaml:"consecutive_failures"`   // Default: 3
	EvaluationWindow     time.Duration `json:"evaluation_window" yaml:"evaluation_window"`         // Default: 5m
	CooldownPeriod       time.Duration `json:"cooldown_period" yaml:"cooldown_period"`             // Default: 2m
	
	// Recovery detection
	RecoveryThreshold    float64       `json:"recovery_threshold" yaml:"recovery_threshold"`       // Default: 0.98 (98% success rate)
	RecoveryDuration     time.Duration `json:"recovery_duration" yaml:"recovery_duration"`         // Default: 1m
}

// Validate checks the backend trigger configuration
func (c *BackendTriggerConfig) Validate() error {
	if c.LatencyWarningMs <= 0 || c.LatencyCriticalMs <= c.LatencyWarningMs || c.LatencyEmergencyMs <= c.LatencyCriticalMs {
		return fmt.Errorf("latency thresholds must be positive and increasing")
	}
	
	if c.ErrorRateWarning < 0 || c.ErrorRateWarning > 1 {
		return fmt.Errorf("error_rate_warning must be between 0 and 1")
	}
	if c.ErrorRateCritical < c.ErrorRateWarning || c.ErrorRateCritical > 1 {
		return fmt.Errorf("error_rate_critical must be >= warning and <= 1")
	}
	if c.ErrorRateEmergency < c.ErrorRateCritical || c.ErrorRateEmergency > 1 {
		return fmt.Errorf("error_rate_emergency must be >= critical and <= 1")
	}
	
	if c.SuccessRateWarning < 0 || c.SuccessRateWarning > 1 {
		return fmt.Errorf("success_rate_warning must be between 0 and 1")
	}
	if c.SuccessRateCritical > c.SuccessRateWarning || c.SuccessRateCritical < 0 {
		return fmt.Errorf("success_rate_critical must be <= warning and >= 0")
	}
	if c.SuccessRateEmergency > c.SuccessRateCritical || c.SuccessRateEmergency < 0 {
		return fmt.Errorf("success_rate_emergency must be <= critical and >= 0")
	}
	
	if c.ConsecutiveFailures <= 0 {
		return fmt.Errorf("consecutive_failures must be positive")
	}
	if c.EvaluationWindow <= 0 {
		return fmt.Errorf("evaluation_window must be positive")
	}
	if c.CooldownPeriod <= 0 {
		return fmt.Errorf("cooldown_period must be positive")
	}
	if c.RecoveryThreshold < 0 || c.RecoveryThreshold > 1 {
		return fmt.Errorf("recovery_threshold must be between 0 and 1")
	}
	if c.RecoveryDuration <= 0 {
		return fmt.Errorf("recovery_duration must be positive")
	}
	
	return nil
}

// SetDefaults sets default values for the backend trigger configuration
func (c *BackendTriggerConfig) SetDefaults() {
	if c.LatencyWarningMs == 0 {
		c.LatencyWarningMs = 5000 // 5 seconds
	}
	if c.LatencyCriticalMs == 0 {
		c.LatencyCriticalMs = 10000 // 10 seconds
	}
	if c.LatencyEmergencyMs == 0 {
		c.LatencyEmergencyMs = 30000 // 30 seconds
	}
	
	if c.ErrorRateWarning == 0 {
		c.ErrorRateWarning = 0.05 // 5%
	}
	if c.ErrorRateCritical == 0 {
		c.ErrorRateCritical = 0.10 // 10%
	}
	if c.ErrorRateEmergency == 0 {
		c.ErrorRateEmergency = 0.25 // 25%
	}
	
	if c.SuccessRateWarning == 0 {
		c.SuccessRateWarning = 0.95 // 95%
	}
	if c.SuccessRateCritical == 0 {
		c.SuccessRateCritical = 0.90 // 90%
	}
	if c.SuccessRateEmergency == 0 {
		c.SuccessRateEmergency = 0.75 // 75%
	}
	
	if c.ConsecutiveFailures == 0 {
		c.ConsecutiveFailures = 3
	}
	if c.EvaluationWindow == 0 {
		c.EvaluationWindow = 5 * time.Minute
	}
	if c.CooldownPeriod == 0 {
		c.CooldownPeriod = 2 * time.Minute
	}
	if c.RecoveryThreshold == 0 {
		c.RecoveryThreshold = 0.98 // 98%
	}
	if c.RecoveryDuration == 0 {
		c.RecoveryDuration = 1 * time.Minute
	}
}

// BackendHealthStatus tracks the health status of backend exporters
type BackendHealthStatus int8

const (
	BackendHealthUnknown BackendHealthStatus = iota
	BackendHealthHealthy
	BackendHealthWarning
	BackendHealthCritical
	BackendHealthUnavailable
)

// String returns the string representation of the backend health status
func (bhs BackendHealthStatus) String() string {
	switch bhs {
	case BackendHealthUnknown:
		return "unknown"
	case BackendHealthHealthy:
		return "healthy"
	case BackendHealthWarning:
		return "warning"
	case BackendHealthCritical:
		return "critical"
	case BackendHealthUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

// BackendHealthHistory tracks recent backend health metrics
type BackendHealthHistory struct {
	WindowStart      time.Time
	WindowEnd        time.Time
	TotalRequests    int64
	SuccessfulReqs   int64
	FailedRequests   int64
	AverageLatency   time.Duration
	MaxLatency       time.Duration
	ErrorRate        float64
	SuccessRate      float64
}

// BackendTrigger monitors backend exporter health and triggers spill operations
// when backends become unavailable or degraded.
// Implements the SpillTrigger interface following the Single Responsibility Principle.
type BackendTrigger struct {
	config            BackendTriggerConfig
	healthStatus      BackendHealthStatus
	lastTrigger       time.Time
	consecutiveIssues int
	lastHealthy       time.Time
	history           []BackendHealthHistory
	mu                sync.RWMutex
	closed            bool
}

// NewBackendTrigger creates a new backend unavailability trigger
func NewBackendTrigger(config BackendTriggerConfig) (*BackendTrigger, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid backend trigger config: %w", err)
	}
	
	return &BackendTrigger{
		config:       config,
		healthStatus: BackendHealthUnknown,
		lastHealthy:  time.Now(),
		history:      make([]BackendHealthHistory, 0, 10),
	}, nil
}

// Evaluate assesses current backend health and determines if spill should be triggered
func (bt *BackendTrigger) Evaluate(ctx context.Context, metrics SystemMetrics) (*TriggerContext, error) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	
	if bt.closed || !bt.config.Enabled {
		return nil, nil
	}
	
	// Update health history
	bt.updateHealthHistory(metrics)
	
	// Calculate current health status
	newStatus := bt.calculateHealthStatus(metrics)
	previousStatus := bt.healthStatus
	bt.healthStatus = newStatus
	
	// Check if we've recovered
	if bt.hasRecovered(newStatus, previousStatus) {
		bt.consecutiveIssues = 0
		bt.lastHealthy = time.Now()
		return nil, nil // No trigger needed, backend has recovered
	}
	
	// Track consecutive issues
	if newStatus != BackendHealthHealthy && newStatus != BackendHealthUnknown {
		bt.consecutiveIssues++
	} else if newStatus == BackendHealthHealthy {
		bt.consecutiveIssues = 0
	}
	
	// Check if we should trigger spill
	if trigger := bt.shouldTriggerSpill(newStatus, metrics); trigger != nil {
		bt.lastTrigger = time.Now()
		return trigger, nil
	}
	
	return nil, nil
}

// updateHealthHistory adds current metrics to the health history
func (bt *BackendTrigger) updateHealthHistory(metrics SystemMetrics) {
	now := time.Now()
	windowStart := now.Add(-bt.config.EvaluationWindow)
	
	// Remove old entries outside the evaluation window
	validHistory := make([]BackendHealthHistory, 0, len(bt.history))
	for _, entry := range bt.history {
		if entry.WindowEnd.After(windowStart) {
			validHistory = append(validHistory, entry)
		}
	}
	
	// Add current metrics
	history := BackendHealthHistory{
		WindowStart:      windowStart,
		WindowEnd:        now,
		AverageLatency:   metrics.ExporterLatency,
		ErrorRate:        1.0 - metrics.ExporterSuccessRate, // Convert success rate to error rate
		SuccessRate:      metrics.ExporterSuccessRate,
		TotalRequests:    metrics.ExporterErrors + int64(float64(metrics.ExporterErrors)/(1.0-metrics.ExporterSuccessRate)), // Estimate total
		FailedRequests:   metrics.ExporterErrors,
	}
	
	// Calculate successful requests
	if history.ErrorRate > 0 && history.ErrorRate < 1.0 {
		history.SuccessfulReqs = history.TotalRequests - history.FailedRequests
	}
	
	validHistory = append(validHistory, history)
	bt.history = validHistory
}

// calculateHealthStatus determines the current backend health status
func (bt *BackendTrigger) calculateHealthStatus(metrics SystemMetrics) BackendHealthStatus {
	latencyMs := metrics.ExporterLatency.Milliseconds()
	errorRate := 1.0 - metrics.ExporterSuccessRate
	successRate := metrics.ExporterSuccessRate
	
	// Check for emergency conditions (highest priority)
	if latencyMs >= bt.config.LatencyEmergencyMs || 
	   errorRate >= bt.config.ErrorRateEmergency || 
	   successRate <= bt.config.SuccessRateEmergency {
		return BackendHealthUnavailable
	}
	
	// Check for critical conditions
	if latencyMs >= bt.config.LatencyCriticalMs || 
	   errorRate >= bt.config.ErrorRateCritical || 
	   successRate <= bt.config.SuccessRateCritical {
		return BackendHealthCritical
	}
	
	// Check for warning conditions
	if latencyMs >= bt.config.LatencyWarningMs || 
	   errorRate >= bt.config.ErrorRateWarning || 
	   successRate <= bt.config.SuccessRateWarning {
		return BackendHealthWarning
	}
	
	return BackendHealthHealthy
}

// hasRecovered checks if the backend has recovered from previous issues
func (bt *BackendTrigger) hasRecovered(newStatus, previousStatus BackendHealthStatus) bool {
	// Must have previous issues to recover from
	if previousStatus == BackendHealthHealthy || bt.consecutiveIssues == 0 {
		return false
	}
	
	// New status must be healthy
	if newStatus != BackendHealthHealthy {
		return false
	}
	
	// Must maintain good health for recovery duration
	if len(bt.history) == 0 {
		return false
	}
	
	// Check if we've had good metrics for the recovery duration
	recoveryStart := time.Now().Add(-bt.config.RecoveryDuration)
	for _, entry := range bt.history {
		if entry.WindowEnd.After(recoveryStart) && entry.SuccessRate < bt.config.RecoveryThreshold {
			return false // Still seeing issues within recovery window
		}
	}
	
	return true
}

// shouldTriggerSpill determines if current conditions warrant a spill trigger
func (bt *BackendTrigger) shouldTriggerSpill(status BackendHealthStatus, metrics SystemMetrics) *TriggerContext {
	// Check cooldown period
	if time.Since(bt.lastTrigger) < bt.config.CooldownPeriod {
		return nil
	}
	
	// Only trigger on degraded backends
	if status == BackendHealthHealthy || status == BackendHealthUnknown {
		return nil
	}
	
	// Check if we've hit consecutive failure threshold
	if bt.consecutiveIssues < bt.config.ConsecutiveFailures && status != BackendHealthUnavailable {
		return nil // Wait for more consecutive issues unless completely unavailable
	}
	
	var severity TriggerSeverity
	var reason string
	
	switch status {
	case BackendHealthWarning:
		severity = TriggerSeverityLow
		reason = fmt.Sprintf("Backend degraded (consecutive issues: %d)", bt.consecutiveIssues)
	case BackendHealthCritical:
		severity = TriggerSeverityMedium
		reason = fmt.Sprintf("Backend critically degraded (consecutive issues: %d)", bt.consecutiveIssues)
	case BackendHealthUnavailable:
		severity = TriggerSeverityHigh
		reason = fmt.Sprintf("Backend unavailable (consecutive issues: %d)", bt.consecutiveIssues)
	default:
		return nil
	}
	
	latencyMs := metrics.ExporterLatency.Milliseconds()
	errorRate := 1.0 - metrics.ExporterSuccessRate
	
	return &TriggerContext{
		Event:       TriggerEventBackendUnavailable,
		Severity:    severity,
		Reason:      reason,
		TriggeredAt: time.Now(),
		Metrics: map[string]float64{
			"exporter_latency_ms":          float64(latencyMs),
			"exporter_error_rate":          errorRate,
			"exporter_success_rate":        metrics.ExporterSuccessRate,
			"consecutive_issues":           float64(bt.consecutiveIssues),
			"time_since_healthy_seconds":   time.Since(bt.lastHealthy).Seconds(),
		},
	}
}

// GetType returns the type of events this trigger monitors
func (bt *BackendTrigger) GetType() TriggerEvent {
	return TriggerEventBackendUnavailable
}

// IsEnabled returns true if this trigger is currently active
func (bt *BackendTrigger) IsEnabled() bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.config.Enabled && !bt.closed
}

// GetHealthStatus returns the current backend health status
func (bt *BackendTrigger) GetHealthStatus() BackendHealthStatus {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.healthStatus
}

// GetHealthHistory returns a copy of the recent health history
func (bt *BackendTrigger) GetHealthHistory() []BackendHealthHistory {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	
	history := make([]BackendHealthHistory, len(bt.history))
	copy(history, bt.history)
	return history
}

// Close releases any resources held by the trigger
func (bt *BackendTrigger) Close() error {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.closed = true
	return nil
}