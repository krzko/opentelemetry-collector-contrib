// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ScriptRetryManager handles Lua script execution with automatic retries and reloading
type ScriptRetryManager struct {
	client RedisClient
	config RetryConfig
	logger *zap.Logger
	
	// Script SHA cache
	scriptSHAs map[string]string
	scripts    map[string]string
}

// NewScriptRetryManager creates a new script retry manager
func NewScriptRetryManager(client RedisClient, config RetryConfig, logger *zap.Logger) *ScriptRetryManager {
	if logger == nil {
		logger = zap.L()
	}
	
	return &ScriptRetryManager{
		client:     client,
		config:     config,
		logger:     logger.With(zap.String("component", "script-retry-manager")),
		scriptSHAs: make(map[string]string),
		scripts:    make(map[string]string),
	}
}

// LoadScript loads a Lua script and returns its SHA
func (srm *ScriptRetryManager) LoadScript(ctx context.Context, name, script string) (string, error) {
	// Store script for potential reloading
	srm.scripts[name] = script
	
	// Load script with retries
	var sha string
	err := srm.retryWithBackoff(ctx, func() error {
		var err error
		sha, err = srm.client.ScriptLoad(ctx, script)
		if err != nil {
			return fmt.Errorf("failed to load script %s: %w", name, err)
		}
		return nil
	})
	
	if err != nil {
		return "", err
	}
	
	// Cache SHA
	srm.scriptSHAs[name] = sha
	srm.logger.Debug("Script loaded successfully",
		zap.String("script", name),
		zap.String("sha", sha))
	
	return sha, nil
}

// ExecuteScript executes a Lua script with automatic retries and reloading on NOSCRIPT errors
func (srm *ScriptRetryManager) ExecuteScript(ctx context.Context, name string, keys []string, args []interface{}) (interface{}, error) {
	sha, exists := srm.scriptSHAs[name]
	if !exists {
		return nil, fmt.Errorf("script %s not loaded", name)
	}
	
	script, hasScript := srm.scripts[name]
	if !hasScript {
		return nil, fmt.Errorf("script %s source not found", name)
	}
	
	var result interface{}
	err := srm.retryWithBackoff(ctx, func() error {
		var err error
		result, err = srm.client.EvalSHA(ctx, sha, keys, args)
		
		if err != nil {
			// Check for NOSCRIPT error
			if isNoScriptError(err) {
				srm.logger.Warn("Script not found in Redis, reloading",
					zap.String("script", name),
					zap.String("sha", sha))
				
				// Reload the script
				newSHA, reloadErr := srm.client.ScriptLoad(ctx, script)
				if reloadErr != nil {
					return fmt.Errorf("failed to reload script %s: %w", name, reloadErr)
				}
				
				// Update cached SHA
				srm.scriptSHAs[name] = newSHA
				sha = newSHA
				
				// Retry execution with new SHA
				result, err = srm.client.EvalSHA(ctx, sha, keys, args)
				if err != nil {
					return fmt.Errorf("failed to execute reloaded script %s: %w", name, err)
				}
				
				return nil
			}
			
			// Check if error is circuit breaker open (should not be wrapped)
			if errors.Is(err, ErrCircuitOpen) {
				return err
			}
			
			// Check if error is retryable
			if !isRetryableError(err) {
				// Non-retryable error, don't wrap it, let retryWithBackoff handle it
				return err
			}
			
			return fmt.Errorf("script execution failed for %s: %w", name, err)
		}
		
		return nil
	})
	
	return result, err
}

// retryWithBackoff implements exponential backoff retry logic
func (srm *ScriptRetryManager) retryWithBackoff(ctx context.Context, fn func() error) error {
	if !srm.config.Enabled {
		// Retries disabled, execute once
		return fn()
	}
	
	backoff := srm.config.InitialBackoff
	
	for attempt := 1; attempt <= srm.config.MaxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		
		// Check if context is cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}
		
		// Check if error is circuit breaker open
		if errors.Is(err, ErrCircuitOpen) {
			// Don't retry on circuit breaker open
			return err
		}
		
		// Check if error is retryable (for non-wrapped errors)
		if !isRetryableError(err) {
			// Non-retryable error, return immediately
			srm.logger.Warn("Non-retryable error encountered",
				zap.Int("attempt", attempt),
				zap.Error(err))
			return fmt.Errorf("non-retryable script error: %w", err)
		}
		
		// Last attempt, return error
		if attempt == srm.config.MaxAttempts {
			srm.logger.Error("Script execution failed after max attempts",
				zap.Int("attempts", attempt),
				zap.Error(err))
			return err
		}
		
		// Log retry attempt
		srm.logger.Warn("Script execution failed, retrying",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(err))
		
		// Wait with backoff
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		
		// Increase backoff
		backoff = time.Duration(float64(backoff) * srm.config.BackoffMultiplier)
		if backoff > srm.config.MaxBackoff {
			backoff = srm.config.MaxBackoff
		}
	}
	
	return fmt.Errorf("retry logic error: should not reach here")
}

// isNoScriptError checks if the error is a NOSCRIPT error
func isNoScriptError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for Redis NOSCRIPT error
	errStr := err.Error()
	return strings.Contains(errStr, "NOSCRIPT") || 
		strings.Contains(errStr, "No matching script")
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// Don't retry on context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	
	// Don't retry on Redis nil (key not found)
	if errors.Is(err, redis.Nil) {
		return false
	}
	
	errStr := err.Error()
	
	// Non-retryable Redis errors
	nonRetryable := []string{
		"WRONGTYPE",     // Wrong data type
		"ERR",           // Generic error (usually logic error)
		"READONLY",      // Replica in read-only mode
		"MASTERDOWN",    // Master is down
		"NOREPLICAS",    // Not enough replicas
		"NOSCRIPT",      // Script not found (handled separately)
	}
	
	for _, pattern := range nonRetryable {
		if strings.Contains(errStr, pattern) {
			return false
		}
	}
	
	// Retryable patterns
	retryable := []string{
		"connection",
		"timeout",
		"refused",
		"reset",
		"broken pipe",
		"EOF",
		"LOADING",      // Redis is loading dataset
		"BUSY",         // Redis is busy
		"CLUSTERDOWN",  // Cluster is down
	}
	
	for _, pattern := range retryable {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return true
		}
	}
	
	// Default to retryable for unknown errors
	return true
}

// LoadAllScripts loads all required Lua scripts
func (srm *ScriptRetryManager) LoadAllScripts(ctx context.Context) error {
	scripts := map[string]string{
		"append_and_index": appendAndIndexScript,
		"lease_acquire":    leaseAcquireScript,
		"lease_release":    leaseReleaseScript,
		"decision_set":     decisionSetScript,
		"spill_add_segment": spillAddSegmentScript,
		"complete_and_gc":  completeAndGCScript,
	}
	
	for name, script := range scripts {
		if _, err := srm.LoadScript(ctx, name, script); err != nil {
			return fmt.Errorf("failed to load script %s: %w", name, err)
		}
	}
	
	srm.logger.Info("All Lua scripts loaded successfully",
		zap.Int("count", len(scripts)))
	
	return nil
}