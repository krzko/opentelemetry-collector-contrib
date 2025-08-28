// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// MetricsCollectorConfig configures the metrics collector behavior
type MetricsCollectorConfig struct {
	// Collection settings
	CollectionInterval time.Duration `json:"collection_interval" yaml:"collection_interval"` // Default: 30s
	
	// Redis metrics collection (optional)
	RedisEnabled  bool `json:"redis_enabled" yaml:"redis_enabled"`
	RedisEndpoint string `json:"redis_endpoint" yaml:"redis_endpoint"`
	
	// Process metrics collection
	ProcessMetricsEnabled bool `json:"process_metrics_enabled" yaml:"process_metrics_enabled"` // Default: true
	
	// Trace buffer metrics collection
	TraceBufferEnabled bool `json:"trace_buffer_enabled" yaml:"trace_buffer_enabled"` // Default: true
	
	// Backend health metrics collection
	BackendHealthEnabled bool `json:"backend_health_enabled" yaml:"backend_health_enabled"` // Default: true
}

// SetDefaults sets default values for the metrics collector configuration
func (c *MetricsCollectorConfig) SetDefaults() {
	if c.CollectionInterval == 0 {
		c.CollectionInterval = 30 * time.Second
	}
	if !c.ProcessMetricsEnabled && !c.TraceBufferEnabled && !c.BackendHealthEnabled {
		// Enable defaults if nothing is explicitly enabled
		c.ProcessMetricsEnabled = true
		c.TraceBufferEnabled = true
		c.BackendHealthEnabled = true
	}
}

// Validate checks the metrics collector configuration
func (c *MetricsCollectorConfig) Validate() error {
	if c.CollectionInterval <= 0 {
		return fmt.Errorf("collection_interval must be positive")
	}
	if c.RedisEnabled && c.RedisEndpoint == "" {
		return fmt.Errorf("redis_endpoint required when redis_enabled is true")
	}
	return nil
}

// MetricsSource provides metrics from different components.
// Following the Interface Segregation Principle, each source
// provides specific types of metrics.
type MetricsSource interface {
	// GetName returns the name of this metrics source
	GetName() string
	
	// IsEnabled returns true if this source is active
	IsEnabled() bool
	
	// CollectMetrics gathers metrics from this source
	CollectMetrics(ctx context.Context) (map[string]interface{}, error)
}

// TraceBufferMetricsSource provides metrics about trace buffer state
type TraceBufferMetricsSource interface {
	MetricsSource
	
	// GetTraceMetrics returns per-trace metrics
	GetTraceMetrics(ctx context.Context) (map[string]TraceMetrics, error)
	
	// GetBufferStats returns overall buffer statistics
	GetBufferStats(ctx context.Context) (BufferStats, error)
}

// BufferStats provides overall buffer statistics
type BufferStats struct {
	ActiveTraces    int
	TotalSpans      int64
	TotalBytes      int64
	OldestTraceAge  time.Duration
	AverageTraceSize int64
}

// RedisMetricsSource provides Redis-specific metrics
type RedisMetricsSource interface {
	MetricsSource
	
	// GetRedisInfo returns Redis server information
	GetRedisInfo(ctx context.Context) (RedisInfo, error)
}

// RedisInfo contains Redis server metrics
type RedisInfo struct {
	MemoryUsed       int64
	MemoryMax        int64
	Connections      int
	CommandLatency   time.Duration
	ErrorCount       int64
}

// BackendMetricsSource provides backend exporter metrics
type BackendMetricsSource interface {
	MetricsSource
	
	// GetBackendHealth returns backend health metrics
	GetBackendHealth(ctx context.Context) (BackendHealth, error)
}

// BackendHealth contains backend health metrics
type BackendHealth struct {
	AverageLatency time.Duration
	ErrorCount     int64
	SuccessRate    float64
	LastError      error
	LastSuccess    time.Time
}

// DefaultMetricsCollector implements the MetricsCollector interface.
// Following the Dependency Inversion Principle, it depends on
// MetricsSource abstractions rather than concrete implementations.
type DefaultMetricsCollector struct {
	config        MetricsCollectorConfig
	sources       map[string]MetricsSource
	lastMetrics   SystemMetrics
	lastCollection time.Time
	collecting    bool
	stopCh        chan struct{}
	mu            sync.RWMutex
}

// NewDefaultMetricsCollector creates a new default metrics collector
func NewDefaultMetricsCollector(config MetricsCollectorConfig) (*DefaultMetricsCollector, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid metrics collector config: %w", err)
	}
	
	return &DefaultMetricsCollector{
		config:  config,
		sources: make(map[string]MetricsSource),
		stopCh:  make(chan struct{}),
	}, nil
}

// RegisterSource adds a metrics source to the collector
func (dmc *DefaultMetricsCollector) RegisterSource(source MetricsSource) error {
	dmc.mu.Lock()
	defer dmc.mu.Unlock()
	
	name := source.GetName()
	if _, exists := dmc.sources[name]; exists {
		return fmt.Errorf("metrics source %s already registered", name)
	}
	
	dmc.sources[name] = source
	return nil
}

// UnregisterSource removes a metrics source from the collector
func (dmc *DefaultMetricsCollector) UnregisterSource(name string) error {
	dmc.mu.Lock()
	defer dmc.mu.Unlock()
	
	if _, exists := dmc.sources[name]; !exists {
		return fmt.Errorf("metrics source %s not found", name)
	}
	
	delete(dmc.sources, name)
	return nil
}

// CollectMetrics gathers current system metrics from all sources
func (dmc *DefaultMetricsCollector) CollectMetrics(ctx context.Context) (SystemMetrics, error) {
	dmc.mu.Lock()
	defer dmc.mu.Unlock()
	
	metrics := SystemMetrics{
		TraceMetrics: make(map[string]TraceMetrics),
		CollectedAt:  time.Now(),
	}
	
	// Collect from all registered sources
	for name, source := range dmc.sources {
		if !source.IsEnabled() {
			continue
		}
		
		sourceMetrics, err := source.CollectMetrics(ctx)
		if err != nil {
			// Log error but continue with other sources
			continue
		}
		
		// Merge source metrics into system metrics
		if err := dmc.mergeSourceMetrics(name, sourceMetrics, &metrics); err != nil {
			continue
		}
	}
	
	// Collect specialized metrics from known source types
	if err := dmc.collectSpecializedMetrics(ctx, &metrics); err != nil {
		return metrics, fmt.Errorf("failed to collect specialized metrics: %w", err)
	}
	
	// Collect process metrics if enabled
	if dmc.config.ProcessMetricsEnabled {
		dmc.collectProcessMetrics(&metrics)
	}
	
	// Update last collected metrics
	dmc.lastMetrics = metrics
	dmc.lastCollection = time.Now()
	
	return metrics, nil
}

// mergeSourceMetrics merges metrics from a specific source into system metrics
func (dmc *DefaultMetricsCollector) mergeSourceMetrics(sourceName string, sourceMetrics map[string]interface{}, systemMetrics *SystemMetrics) error {
	// Handle Redis metrics
	if sourceName == "redis" && dmc.config.RedisEnabled {
		if memUsed, ok := sourceMetrics["memory_used"].(int64); ok {
			systemMetrics.RedisMemoryUsed = memUsed
		}
		if memMax, ok := sourceMetrics["memory_max"].(int64); ok {
			systemMetrics.RedisMemoryMax = memMax
		}
		if connections, ok := sourceMetrics["connections"].(int); ok {
			systemMetrics.RedisConnections = connections
		}
		if latency, ok := sourceMetrics["command_latency"].(time.Duration); ok {
			systemMetrics.RedisCommandLatency = latency
		}
		if errors, ok := sourceMetrics["errors"].(int64); ok {
			systemMetrics.RedisErrors = errors
		}
	}
	
	// Handle backend metrics
	if sourceName == "backend" && dmc.config.BackendHealthEnabled {
		if latency, ok := sourceMetrics["average_latency"].(time.Duration); ok {
			systemMetrics.ExporterLatency = latency
		}
		if errors, ok := sourceMetrics["error_count"].(int64); ok {
			systemMetrics.ExporterErrors = errors
		}
		if successRate, ok := sourceMetrics["success_rate"].(float64); ok {
			systemMetrics.ExporterSuccessRate = successRate
		}
	}
	
	return nil
}

// collectSpecializedMetrics collects metrics from specialized source interfaces
func (dmc *DefaultMetricsCollector) collectSpecializedMetrics(ctx context.Context, metrics *SystemMetrics) error {
	// Collect trace buffer metrics
	for _, source := range dmc.sources {
		if tbSource, ok := source.(TraceBufferMetricsSource); ok && tbSource.IsEnabled() {
			// Get per-trace metrics
			traceMetrics, err := tbSource.GetTraceMetrics(ctx)
			if err == nil {
				for traceID, tm := range traceMetrics {
					metrics.TraceMetrics[traceID] = tm
				}
			}
			
			// Get buffer stats
			bufferStats, err := tbSource.GetBufferStats(ctx)
			if err == nil {
				metrics.ActiveTraces = bufferStats.ActiveTraces
				metrics.TotalSpans = bufferStats.TotalSpans
				metrics.TotalBytes = bufferStats.TotalBytes
				metrics.OldestTraceAge = bufferStats.OldestTraceAge
				metrics.AverageTraceSize = bufferStats.AverageTraceSize
			}
		}
		
		if redisSource, ok := source.(RedisMetricsSource); ok && redisSource.IsEnabled() {
			redisInfo, err := redisSource.GetRedisInfo(ctx)
			if err == nil {
				metrics.RedisMemoryUsed = redisInfo.MemoryUsed
				metrics.RedisMemoryMax = redisInfo.MemoryMax
				metrics.RedisConnections = redisInfo.Connections
				metrics.RedisCommandLatency = redisInfo.CommandLatency
				metrics.RedisErrors = redisInfo.ErrorCount
			}
		}
		
		if backendSource, ok := source.(BackendMetricsSource); ok && backendSource.IsEnabled() {
			backendHealth, err := backendSource.GetBackendHealth(ctx)
			if err == nil {
				metrics.ExporterLatency = backendHealth.AverageLatency
				metrics.ExporterErrors = backendHealth.ErrorCount
				metrics.ExporterSuccessRate = backendHealth.SuccessRate
			}
		}
	}
	
	return nil
}

// collectProcessMetrics gathers process-level metrics
func (dmc *DefaultMetricsCollector) collectProcessMetrics(metrics *SystemMetrics) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Estimate process memory usage
	metrics.ProcessMemoryUsed = int64(memStats.Sys)
	
	// Note: ProcessMemoryMax would need to be configured or detected
	// from container limits or system settings
	
	// Estimate CPU usage (simplified)
	// In a real implementation, this would track CPU time over intervals
	metrics.ProcessCPUUsage = 0.0 // TODO: Implement proper CPU tracking
}

// StartCollection begins periodic metrics collection
func (dmc *DefaultMetricsCollector) StartCollection(ctx context.Context, interval time.Duration) error {
	dmc.mu.Lock()
	defer dmc.mu.Unlock()
	
	if dmc.collecting {
		return fmt.Errorf("collection already started")
	}
	
	if interval == 0 {
		interval = dmc.config.CollectionInterval
	}
	
	dmc.collecting = true
	
	go dmc.collectionLoop(ctx, interval)
	
	return nil
}

// collectionLoop runs the periodic metrics collection
func (dmc *DefaultMetricsCollector) collectionLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-dmc.stopCh:
			return
		case <-ticker.C:
			// Collect metrics with timeout
			collectCtx, cancel := context.WithTimeout(ctx, interval/2)
			_, _ = dmc.CollectMetrics(collectCtx) // Ignore errors in background collection
			cancel()
		}
	}
}

// StopCollection stops periodic metrics collection
func (dmc *DefaultMetricsCollector) StopCollection() error {
	dmc.mu.Lock()
	defer dmc.mu.Unlock()
	
	if !dmc.collecting {
		return nil
	}
	
	close(dmc.stopCh)
	dmc.collecting = false
	dmc.stopCh = make(chan struct{}) // Reset for potential restart
	
	return nil
}

// GetLastMetrics returns the most recently collected metrics
func (dmc *DefaultMetricsCollector) GetLastMetrics() (SystemMetrics, bool) {
	dmc.mu.RLock()
	defer dmc.mu.RUnlock()
	
	if dmc.lastCollection.IsZero() {
		return SystemMetrics{}, false
	}
	
	// Return a copy to avoid race conditions
	metrics := dmc.lastMetrics
	metrics.TraceMetrics = make(map[string]TraceMetrics)
	for k, v := range dmc.lastMetrics.TraceMetrics {
		metrics.TraceMetrics[k] = v
	}
	
	return metrics, true
}

// GetSources returns all registered metrics sources
func (dmc *DefaultMetricsCollector) GetSources() map[string]MetricsSource {
	dmc.mu.RLock()
	defer dmc.mu.RUnlock()
	
	sources := make(map[string]MetricsSource)
	for name, source := range dmc.sources {
		sources[name] = source
	}
	
	return sources
}

// IsCollecting returns true if periodic collection is active
func (dmc *DefaultMetricsCollector) IsCollecting() bool {
	dmc.mu.RLock()
	defer dmc.mu.RUnlock()
	return dmc.collecting
}