// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// SpillPolicyConfig configures the spill policy behavior
type SpillPolicyConfig struct {
	// Enabled determines if spill policy is active
	Enabled bool `json:"enabled" yaml:"enabled"`
	
	// Evaluation settings
	EvaluationInterval time.Duration `json:"evaluation_interval" yaml:"evaluation_interval"` // Default: 30s
	
	// Spill target selection strategy
	SelectionStrategy string `json:"selection_strategy" yaml:"selection_strategy"` // "oldest", "largest", "priority", "all"
	
	// Maximum targets per spill operation
	MaxTargetsPerSpill int `json:"max_targets_per_spill" yaml:"max_targets_per_spill"` // Default: 100
	
	// Minimum defer time when not spilling
	MinDeferTime time.Duration `json:"min_defer_time" yaml:"min_defer_time"` // Default: 30s
	MaxDeferTime time.Duration `json:"max_defer_time" yaml:"max_defer_time"` // Default: 5m
	
	// Global spill protection
	MaxSpillsPerHour int `json:"max_spills_per_hour" yaml:"max_spills_per_hour"` // Default: 100
	
	// Advanced configuration
	RequireConsensus  bool    `json:"require_consensus" yaml:"require_consensus"`   // Default: false
	ConsensusRatio    float64 `json:"consensus_ratio" yaml:"consensus_ratio"`       // Default: 0.5 (50%)
}

// Validate checks the spill policy configuration
func (c *SpillPolicyConfig) Validate() error {
	if c.EvaluationInterval <= 0 {
		return fmt.Errorf("evaluation_interval must be positive")
	}
	if c.MaxTargetsPerSpill <= 0 {
		return fmt.Errorf("max_targets_per_spill must be positive")
	}
	if c.MinDeferTime <= 0 {
		return fmt.Errorf("min_defer_time must be positive")
	}
	if c.MaxDeferTime <= c.MinDeferTime {
		return fmt.Errorf("max_defer_time must be greater than min_defer_time")
	}
	if c.MaxSpillsPerHour <= 0 {
		return fmt.Errorf("max_spills_per_hour must be positive")
	}
	if c.ConsensusRatio < 0 || c.ConsensusRatio > 1 {
		return fmt.Errorf("consensus_ratio must be between 0 and 1")
	}
	
	validStrategies := map[string]bool{
		"oldest":   true,
		"largest":  true,
		"priority": true,
		"all":      true,
	}
	if !validStrategies[c.SelectionStrategy] {
		return fmt.Errorf("invalid selection_strategy: %s", c.SelectionStrategy)
	}
	
	return nil
}

// SetDefaults sets default values for the spill policy configuration
func (c *SpillPolicyConfig) SetDefaults() {
	if c.EvaluationInterval == 0 {
		c.EvaluationInterval = 30 * time.Second
	}
	if c.SelectionStrategy == "" {
		c.SelectionStrategy = "priority"
	}
	if c.MaxTargetsPerSpill == 0 {
		c.MaxTargetsPerSpill = 100
	}
	if c.MinDeferTime == 0 {
		c.MinDeferTime = 30 * time.Second
	}
	if c.MaxDeferTime == 0 {
		c.MaxDeferTime = 5 * time.Minute
	}
	if c.MaxSpillsPerHour == 0 {
		c.MaxSpillsPerHour = 100
	}
	if c.ConsensusRatio == 0 {
		c.ConsensusRatio = 0.5
	}
}

// SpillPolicyStats tracks spill policy performance metrics
type SpillPolicyStats struct {
	TotalEvaluations    int64
	TotalSpillsTriggered int64
	TotalTargetsSpilled  int64
	LastEvaluationTime  time.Time
	LastSpillTime       time.Time
	ActiveTriggers      []TriggerEvent
	SpillsThisHour      int
	AverageTargetsPerSpill float64
}

// DefaultSpillPolicy implements the SpillPolicy interface.
// Following the Open/Closed Principle, it can accommodate new triggers
// without modification to the core policy logic.
type DefaultSpillPolicy struct {
	config    SpillPolicyConfig
	triggers  map[TriggerEvent]SpillTrigger
	stats     SpillPolicyStats
	spillHistory []time.Time
	mu        sync.RWMutex
	closed    bool
}

// NewDefaultSpillPolicy creates a new default spill policy
func NewDefaultSpillPolicy(config SpillPolicyConfig) (*DefaultSpillPolicy, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid spill policy config: %w", err)
	}
	
	return &DefaultSpillPolicy{
		config:       config,
		triggers:     make(map[TriggerEvent]SpillTrigger),
		spillHistory: make([]time.Time, 0, config.MaxSpillsPerHour),
	}, nil
}

// EvaluateSpill assesses all triggers and makes a spill decision
func (dsp *DefaultSpillPolicy) EvaluateSpill(ctx context.Context, metrics SystemMetrics) (SpillDecision, error) {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()
	
	if dsp.closed || !dsp.config.Enabled {
		return SpillDecision{
			ShouldSpill: false,
			DeferFor:    dsp.config.MaxDeferTime,
			Reason:      "spill policy disabled",
		}, nil
	}
	
	dsp.stats.TotalEvaluations++
	dsp.stats.LastEvaluationTime = time.Now()
	
	// Check rate limiting
	if dsp.isRateLimited() {
		return SpillDecision{
			ShouldSpill: false,
			DeferFor:    dsp.calculateDeferTime(0),
			Reason:      "rate limited - too many spills this hour",
		}, nil
	}
	
	// Evaluate all triggers
	triggerContexts, err := dsp.evaluateAllTriggers(ctx, metrics)
	if err != nil {
		return SpillDecision{
			ShouldSpill: false,
			DeferFor:    dsp.config.MinDeferTime,
			Reason:      fmt.Sprintf("trigger evaluation error: %v", err),
		}, err
	}
	
	// Update active triggers
	dsp.updateActiveTriggers(triggerContexts)
	
	// Make spill decision based on triggers
	decision := dsp.makeSpillDecision(triggerContexts, metrics)
	
	// Update statistics
	if decision.ShouldSpill {
		dsp.stats.TotalSpillsTriggered++
		dsp.stats.TotalTargetsSpilled += int64(len(decision.Targets))
		dsp.stats.LastSpillTime = time.Now()
		dsp.spillHistory = append(dsp.spillHistory, time.Now())
		dsp.updateAverageTargets()
	}
	
	return decision, nil
}

// evaluateAllTriggers runs evaluation on all registered triggers
func (dsp *DefaultSpillPolicy) evaluateAllTriggers(ctx context.Context, metrics SystemMetrics) ([]*TriggerContext, error) {
	var contexts []*TriggerContext
	var evaluationErrors []error
	
	for _, trigger := range dsp.triggers {
		if !trigger.IsEnabled() {
			continue
		}
		
		triggerCtx, err := trigger.Evaluate(ctx, metrics)
		if err != nil {
			evaluationErrors = append(evaluationErrors, fmt.Errorf("trigger %s error: %w", trigger.GetType().String(), err))
			continue
		}
		
		if triggerCtx != nil {
			contexts = append(contexts, triggerCtx)
		}
	}
	
	// Return error if all triggers failed
	if len(evaluationErrors) > 0 && len(contexts) == 0 {
		return nil, fmt.Errorf("all trigger evaluations failed: %v", evaluationErrors)
	}
	
	return contexts, nil
}

// updateActiveTriggers updates the list of currently active triggers
func (dsp *DefaultSpillPolicy) updateActiveTriggers(contexts []*TriggerContext) {
	activeTriggers := make([]TriggerEvent, 0, len(contexts))
	for _, ctx := range contexts {
		activeTriggers = append(activeTriggers, ctx.Event)
	}
	dsp.stats.ActiveTriggers = activeTriggers
}

// makeSpillDecision creates a spill decision based on trigger contexts
func (dsp *DefaultSpillPolicy) makeSpillDecision(contexts []*TriggerContext, metrics SystemMetrics) SpillDecision {
	if len(contexts) == 0 {
		return SpillDecision{
			ShouldSpill: false,
			DeferFor:    dsp.config.MinDeferTime,
			Reason:      "no active triggers",
		}
	}
	
	// Check if consensus is required
	if dsp.config.RequireConsensus && !dsp.hasConsensus(contexts) {
		return SpillDecision{
			ShouldSpill: false,
			DeferFor:    dsp.calculateDeferTime(len(contexts)),
			Reason:      fmt.Sprintf("insufficient consensus: %d triggers active", len(contexts)),
		}
	}
	
	// Determine highest severity
	maxSeverity := TriggerSeverityLow
	var primaryContext *TriggerContext
	for _, ctx := range contexts {
		if ctx.Severity > maxSeverity {
			maxSeverity = ctx.Severity
			primaryContext = ctx
		}
	}
	
	// Generate spill targets
	targets := dsp.generateSpillTargets(contexts, metrics)
	
	reason := fmt.Sprintf("spill triggered by %s (severity: %s)", 
		primaryContext.Event.String(), maxSeverity.String())
	
	return SpillDecision{
		ShouldSpill: true,
		Targets:     targets,
		Reason:      reason,
	}
}

// hasConsensus checks if enough triggers agree on spilling
func (dsp *DefaultSpillPolicy) hasConsensus(contexts []*TriggerContext) bool {
	totalTriggers := len(dsp.triggers)
	activeTriggers := len(contexts)
	
	if totalTriggers == 0 {
		return false
	}
	
	// Use ceiling to ensure proper consensus - if ratio is 0.75 with 2 triggers, need 2 active
	requiredTriggers := float64(totalTriggers) * dsp.config.ConsensusRatio
	consensusThreshold := int(requiredTriggers)
	if requiredTriggers > float64(consensusThreshold) {
		consensusThreshold++ // Round up to ensure proper consensus
	}
	
	return activeTriggers >= consensusThreshold
}

// generateSpillTargets creates spill targets based on triggers and metrics
func (dsp *DefaultSpillPolicy) generateSpillTargets(contexts []*TriggerContext, metrics SystemMetrics) []SpillTarget {
	var targets []SpillTarget
	
	// Check for specific trace targets from triggers
	for _, ctx := range contexts {
		if ctx.TraceID != (TraceID{}) {
			target := SpillTarget{
				TraceID:  ctx.TraceID,
				Tenant:   ctx.Tenant,
				Priority: int(ctx.Severity),
			}
			
			// Add metrics if available
			if traceMetrics, exists := metrics.TraceMetrics[ctx.TraceID.Hex()]; exists {
				target.EstimatedBytes = traceMetrics.BytesUsed
				target.SpanCount = traceMetrics.SpanCount
				target.Age = time.Since(traceMetrics.FirstSeen)
			}
			
			targets = append(targets, target)
		}
	}
	
	// If no specific targets, generate global targets
	if len(targets) == 0 {
		targets = dsp.generateGlobalTargets(metrics)
	}
	
	// Sort and limit targets
	targets = dsp.selectTargets(targets)
	
	return targets
}

// generateGlobalTargets creates targets based on global metrics
func (dsp *DefaultSpillPolicy) generateGlobalTargets(metrics SystemMetrics) []SpillTarget {
	var targets []SpillTarget
	
	for _, traceMetrics := range metrics.TraceMetrics {
		target := SpillTarget{
			TraceID:        traceMetrics.TraceID,
			Tenant:         traceMetrics.Tenant,
			EstimatedBytes: traceMetrics.BytesUsed,
			SpanCount:      traceMetrics.SpanCount,
			Age:            time.Since(traceMetrics.FirstSeen),
			Priority:       dsp.calculateTracePriority(traceMetrics),
		}
		targets = append(targets, target)
		
		// Early termination if we have too many candidates
		if len(targets) > dsp.config.MaxTargetsPerSpill*2 {
			break
		}
	}
	
	return targets
}

// calculateTracePriority determines priority for a trace based on its metrics
func (dsp *DefaultSpillPolicy) calculateTracePriority(traceMetrics TraceMetrics) int {
	priority := 0
	
	// Higher priority for larger traces
	if traceMetrics.BytesUsed > 1024*1024 { // 1MB
		priority += 3
	} else if traceMetrics.BytesUsed > 512*1024 { // 512KB
		priority += 2
	} else if traceMetrics.BytesUsed > 256*1024 { // 256KB
		priority += 1
	}
	
	// Higher priority for older traces
	age := time.Since(traceMetrics.FirstSeen)
	if age > 10*time.Minute {
		priority += 3
	} else if age > 5*time.Minute {
		priority += 2
	} else if age > 2*time.Minute {
		priority += 1
	}
	
	// Higher priority for traces with many spans
	if traceMetrics.SpanCount > 1000 {
		priority += 2
	} else if traceMetrics.SpanCount > 500 {
		priority += 1
	}
	
	return priority
}

// selectTargets sorts and limits targets based on selection strategy
func (dsp *DefaultSpillPolicy) selectTargets(targets []SpillTarget) []SpillTarget {
	if len(targets) == 0 {
		return targets
	}
	
	// Sort based on strategy
	switch dsp.config.SelectionStrategy {
	case "oldest":
		sort.Slice(targets, func(i, j int) bool {
			return targets[i].Age > targets[j].Age
		})
	case "largest":
		sort.Slice(targets, func(i, j int) bool {
			return targets[i].EstimatedBytes > targets[j].EstimatedBytes
		})
	case "priority":
		sort.Slice(targets, func(i, j int) bool {
			return targets[i].Priority > targets[j].Priority
		})
	case "all":
		// Keep all targets, no sorting needed
	}
	
	// Limit number of targets
	if len(targets) > dsp.config.MaxTargetsPerSpill {
		targets = targets[:dsp.config.MaxTargetsPerSpill]
	}
	
	return targets
}

// isRateLimited checks if we've exceeded the hourly spill limit
func (dsp *DefaultSpillPolicy) isRateLimited() bool {
	// Clean up old entries
	hourAgo := time.Now().Add(-time.Hour)
	validSpills := make([]time.Time, 0, len(dsp.spillHistory))
	for _, spillTime := range dsp.spillHistory {
		if spillTime.After(hourAgo) {
			validSpills = append(validSpills, spillTime)
		}
	}
	dsp.spillHistory = validSpills
	dsp.stats.SpillsThisHour = len(validSpills)
	
	return len(validSpills) >= dsp.config.MaxSpillsPerHour
}

// calculateDeferTime determines how long to defer before next evaluation
func (dsp *DefaultSpillPolicy) calculateDeferTime(activeTriggers int) time.Duration {
	// More active triggers = shorter defer time
	if activeTriggers >= 3 {
		return dsp.config.MinDeferTime
	} else if activeTriggers >= 1 {
		return dsp.config.MinDeferTime * 2
	}
	return dsp.config.MaxDeferTime
}

// updateAverageTargets updates the average targets per spill metric
func (dsp *DefaultSpillPolicy) updateAverageTargets() {
	if dsp.stats.TotalSpillsTriggered > 0 {
		dsp.stats.AverageTargetsPerSpill = float64(dsp.stats.TotalTargetsSpilled) / float64(dsp.stats.TotalSpillsTriggered)
	}
}

// RegisterTrigger adds a new trigger to the policy
func (dsp *DefaultSpillPolicy) RegisterTrigger(trigger SpillTrigger) error {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()
	
	if dsp.closed {
		return fmt.Errorf("policy is closed")
	}
	
	triggerType := trigger.GetType()
	if _, exists := dsp.triggers[triggerType]; exists {
		return fmt.Errorf("trigger of type %s already registered", triggerType.String())
	}
	
	dsp.triggers[triggerType] = trigger
	return nil
}

// UnregisterTrigger removes a trigger from the policy
func (dsp *DefaultSpillPolicy) UnregisterTrigger(triggerType TriggerEvent) error {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()
	
	if dsp.closed {
		return fmt.Errorf("policy is closed")
	}
	
	trigger, exists := dsp.triggers[triggerType]
	if !exists {
		return fmt.Errorf("trigger of type %s not found", triggerType.String())
	}
	
	delete(dsp.triggers, triggerType)
	
	// Close the trigger
	return trigger.Close()
}

// GetActiveTriggers returns all currently enabled triggers
func (dsp *DefaultSpillPolicy) GetActiveTriggers() []SpillTrigger {
	dsp.mu.RLock()
	defer dsp.mu.RUnlock()
	
	var activeTriggers []SpillTrigger
	for _, trigger := range dsp.triggers {
		if trigger.IsEnabled() {
			activeTriggers = append(activeTriggers, trigger)
		}
	}
	
	return activeTriggers
}

// GetStats returns current policy statistics
func (dsp *DefaultSpillPolicy) GetStats() SpillPolicyStats {
	dsp.mu.RLock()
	defer dsp.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	stats := dsp.stats
	stats.ActiveTriggers = make([]TriggerEvent, len(dsp.stats.ActiveTriggers))
	copy(stats.ActiveTriggers, dsp.stats.ActiveTriggers)
	
	return stats
}

// Close releases all resources and stops all triggers
func (dsp *DefaultSpillPolicy) Close() error {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()
	
	if dsp.closed {
		return nil
	}
	
	var errors []error
	for _, trigger := range dsp.triggers {
		if err := trigger.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	
	dsp.triggers = nil
	dsp.closed = true
	
	if len(errors) > 0 {
		return fmt.Errorf("errors closing triggers: %v", errors)
	}
	
	return nil
}