// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"time"
)

// TriggerEvent represents the type of event that can trigger a spill operation
type TriggerEvent int8

const (
	// TriggerEventMemoryPressure indicates memory pressure (Redis or local memory)
	TriggerEventMemoryPressure TriggerEvent = iota
	// TriggerEventSizeThreshold indicates a trace has exceeded size limits
	TriggerEventSizeThreshold
	// TriggerEventAgeThreshold indicates a trace has exceeded age limits
	TriggerEventAgeThreshold
	// TriggerEventBackendUnavailable indicates downstream exporters are failing
	TriggerEventBackendUnavailable
	// TriggerEventSystemPressure indicates overall system resource pressure
	TriggerEventSystemPressure
	// TriggerEventManualTrigger indicates a manual spill trigger (for testing/ops)
	TriggerEventManualTrigger
)

// String returns the string representation of the trigger event
func (te TriggerEvent) String() string {
	switch te {
	case TriggerEventMemoryPressure:
		return "memory_pressure"
	case TriggerEventSizeThreshold:
		return "size_threshold"
	case TriggerEventAgeThreshold:
		return "age_threshold"
	case TriggerEventBackendUnavailable:
		return "backend_unavailable"
	case TriggerEventSystemPressure:
		return "system_pressure"
	case TriggerEventManualTrigger:
		return "manual_trigger"
	default:
		return "unknown"
	}
}

// TriggerSeverity indicates the urgency of the spill trigger
type TriggerSeverity int8

const (
	// TriggerSeverityLow indicates a soft suggestion to spill (background operation)
	TriggerSeverityLow TriggerSeverity = iota
	// TriggerSeverityMedium indicates moderate pressure to spill (prioritized operation)
	TriggerSeverityMedium
	// TriggerSeverityHigh indicates urgent need to spill (immediate operation)
	TriggerSeverityHigh
	// TriggerSeverityCritical indicates emergency spill required (may drop data if failed)
	TriggerSeverityCritical
)

// String returns the string representation of the trigger severity
func (ts TriggerSeverity) String() string {
	switch ts {
	case TriggerSeverityLow:
		return "low"
	case TriggerSeverityMedium:
		return "medium"
	case TriggerSeverityHigh:
		return "high"
	case TriggerSeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// TriggerContext provides contextual information about a spill trigger
type TriggerContext struct {
	// Event is the type of event that triggered the spill
	Event TriggerEvent
	// Severity indicates the urgency of the spill operation
	Severity TriggerSeverity
	// TraceID identifies the specific trace (empty for global triggers)
	TraceID TraceID
	// Tenant identifies the tenant (empty for global triggers)
	Tenant Tenant
	// Reason provides human-readable explanation for the trigger
	Reason string
	// Metrics contains numerical context about the trigger
	Metrics map[string]float64
	// TriggeredAt indicates when the trigger was fired
	TriggeredAt time.Time
}

// SpillTarget defines what should be spilled
type SpillTarget struct {
	// TraceID identifies the specific trace to spill
	TraceID TraceID
	// Tenant identifies the tenant
	Tenant Tenant
	// Priority indicates spill priority (higher = more urgent)
	Priority int
	// EstimatedBytes provides size estimate for planning
	EstimatedBytes int64
	// SpanCount provides span count estimate
	SpanCount int
	// Age indicates how old the trace data is
	Age time.Duration
}

// SpillDecision represents the result of evaluating spill triggers
type SpillDecision struct {
	// ShouldSpill indicates whether spill operation should proceed
	ShouldSpill bool
	// Targets lists the specific traces to spill (empty for global spill)
	Targets []SpillTarget
	// DeferFor indicates how long to defer if not spilling now
	DeferFor time.Duration
	// Reason provides explanation for the decision
	Reason string
}

// SpillTrigger evaluates conditions and determines when to trigger spill operations.
// Following the Single Responsibility Principle, each trigger implementation
// focuses on a specific type of condition (memory, size, age, etc.).
type SpillTrigger interface {
	// Evaluate assesses current conditions and determines if spill should be triggered
	// Returns TriggerContext with event details if triggered, nil if no action needed
	Evaluate(ctx context.Context, metrics SystemMetrics) (*TriggerContext, error)

	// GetType returns the type of events this trigger monitors
	GetType() TriggerEvent

	// IsEnabled returns true if this trigger is currently active
	IsEnabled() bool

	// Close releases any resources held by the trigger
	Close() error
}

// SpillPolicy combines multiple triggers and makes spill decisions.
// Following the Open/Closed Principle, new triggers can be added
// without modifying existing policy implementation.
type SpillPolicy interface {
	// EvaluateSpill assesses all triggers and makes a spill decision
	EvaluateSpill(ctx context.Context, metrics SystemMetrics) (SpillDecision, error)

	// RegisterTrigger adds a new trigger to the policy
	RegisterTrigger(trigger SpillTrigger) error

	// UnregisterTrigger removes a trigger from the policy
	UnregisterTrigger(triggerType TriggerEvent) error

	// GetActiveTriggers returns all currently enabled triggers
	GetActiveTriggers() []SpillTrigger

	// Close releases all resources and stops all triggers
	Close() error
}

// SystemMetrics provides system-wide metrics for trigger evaluation
type SystemMetrics struct {
	// Redis metrics
	RedisMemoryUsed        int64              // bytes
	RedisMemoryMax         int64              // bytes  
	RedisConnections       int                // active connections
	RedisCommandLatency    time.Duration      // average command latency
	RedisErrors            int64              // error count
	
	// Trace buffer metrics
	ActiveTraces           int                // number of active traces
	TotalSpans             int64              // total spans across all traces
	TotalBytes             int64              // total memory usage estimate
	AverageTraceSize       int64              // average trace size in bytes
	OldestTraceAge         time.Duration      // age of oldest active trace
	
	// Per-trace metrics (keyed by trace ID hex)
	TraceMetrics          map[string]TraceMetrics
	
	// Backend health metrics
	ExporterLatency       time.Duration      // average exporter latency
	ExporterErrors        int64              // exporter error count
	ExporterSuccessRate   float64            // success rate (0.0-1.0)
	
	// System resource metrics
	ProcessMemoryUsed     int64              // process memory usage
	ProcessMemoryMax      int64              // process memory limit
	ProcessCPUUsage       float64            // CPU usage percentage
	
	// Collected timestamp
	CollectedAt           time.Time
}

// TraceMetrics provides per-trace metrics for trigger evaluation
type TraceMetrics struct {
	TraceID      TraceID
	Tenant       Tenant
	SpanCount    int
	BytesUsed    int64
	FirstSeen    time.Time
	LastUpdate   time.Time
	IsLeased     bool
	State        string // "active", "spilled", "decided", "completed"
}

// MetricsCollector gathers system metrics for trigger evaluation.
// Following the Interface Segregation Principle, this interface
// focuses solely on metrics collection.
type MetricsCollector interface {
	// CollectMetrics gathers current system metrics
	CollectMetrics(ctx context.Context) (SystemMetrics, error)

	// StartCollection begins periodic metrics collection
	StartCollection(ctx context.Context, interval time.Duration) error

	// StopCollection stops periodic metrics collection
	StopCollection() error

	// GetLastMetrics returns the most recently collected metrics
	GetLastMetrics() (SystemMetrics, bool)
}

// SpillCoordinator orchestrates the complete spill trigger and execution process.
// Following the Dependency Inversion Principle, it depends on abstractions
// rather than concrete implementations.
type SpillCoordinator interface {
	// Start begins the spill monitoring and coordination process
	Start(ctx context.Context) error

	// Stop gracefully shuts down spill coordination
	Stop(ctx context.Context) error

	// TriggerManualSpill allows manual triggering of spill operations
	TriggerManualSpill(ctx context.Context, targets []SpillTarget) error

	// GetStatus returns current spill coordinator status
	GetStatus() SpillCoordinatorStatus

	// RegisterPolicy sets the spill policy to use
	RegisterPolicy(policy SpillPolicy) error

	// RegisterCollector sets the metrics collector to use
	RegisterCollector(collector MetricsCollector) error
}

// SpillCoordinatorStatus provides status information about the spill coordinator
type SpillCoordinatorStatus struct {
	IsRunning         bool
	LastEvaluation    time.Time
	LastSpillTrigger  time.Time
	ActiveTriggers    []TriggerEvent
	PendingSpills     int
	TotalSpillsToday  int64
	LastError         error
}