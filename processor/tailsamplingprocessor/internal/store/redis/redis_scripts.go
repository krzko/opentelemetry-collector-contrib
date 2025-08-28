// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// ScriptManager manages Redis Lua scripts and their execution
type ScriptManager struct {
	client RedisClient
	logger *zap.Logger
	
	// Script SHA1 hashes
	appendAndIndexSHA string
	leaseAcquireSHA   string
	leaseReleaseSHA   string
	decisionSetSHA    string
	spillAddSegmentSHA string
	completeAndGCSHA  string
}

// NewScriptManager creates a new script manager
func NewScriptManager(client RedisClient, logger *zap.Logger) *ScriptManager {
	return &ScriptManager{
		client: client,
		logger: logger.With(zap.String("component", "script-manager")),
	}
}

// LoadScripts loads all Lua scripts into Redis
func (sm *ScriptManager) LoadScripts(ctx context.Context) error {
	scripts := map[string]*string{
		"append_and_index": &sm.appendAndIndexSHA,
		"lease_acquire":    &sm.leaseAcquireSHA,
		"lease_release":    &sm.leaseReleaseSHA,
		"decision_set":     &sm.decisionSetSHA,
		"spill_add_segment": &sm.spillAddSegmentSHA,
		"complete_and_gc":  &sm.completeAndGCSHA,
	}
	
	for name, shaPtr := range scripts {
		script := sm.getScript(name)
		sha, err := sm.client.ScriptLoad(ctx, script)
		if err != nil {
			return fmt.Errorf("failed to load script %s: %w", name, err)
		}
		*shaPtr = sha
		sm.logger.Debug("Loaded Lua script", zap.String("name", name), zap.String("sha", sha))
	}
	
	sm.logger.Info("All Lua scripts loaded successfully")
	return nil
}

// AppendAndIndex executes the append_and_index.lua script
func (sm *ScriptManager) AppendAndIndex(ctx context.Context, req AppendRequest) (AppendResponse, error) {
	keys := []string{
		req.Keys.Meta,
		req.Keys.Spans,
		req.Keys.Dedup,
		req.Keys.Lease,
		req.Keys.Active,
		req.Keys.Decision,
	}
	
	args := []interface{}{
		time.Now().UnixMilli(),             // now_ms
		req.TTL.Milliseconds(),            // ttl_ms
		req.DecisionWait.Milliseconds(),   // decision_wait_ms
		req.Tenant,                        // tenant
		fmt.Sprintf("%x", req.TraceID),    // trace_id_member
		req.SpanID,                        // span_id
		string(req.SpanPayload),           // span_payload
		req.SpanPayloadLen,                // span_payload_len
		req.MaxSpansPerTrace,              // max_spans_per_trace
		req.MaxBytesPerTrace,              // max_bytes_per_trace
		req.LeaseOwner,                    // lease_owner
		boolToInt(req.RefreshLease),       // refresh_lease
	}
	
	result, err := sm.client.EvalSHA(ctx, sm.appendAndIndexSHA, keys, args)
	if err != nil {
		return AppendResponse{}, fmt.Errorf("append_and_index script failed: %w", err)
	}
	
	return sm.parseAppendResponse(result)
}

// LeaseAcquire executes the lease_acquire.lua script
func (sm *ScriptManager) LeaseAcquire(ctx context.Context, req LeaseRequest) (bool, error) {
	keys := []string{req.LeaseKey}
	args := []interface{}{
		req.OwnerID,
		req.TTL.Milliseconds(),
		boolToInt(req.Refresh),
		time.Now().UnixMilli(),
	}
	
	result, err := sm.client.EvalSHA(ctx, sm.leaseAcquireSHA, keys, args)
	if err != nil {
		return false, fmt.Errorf("lease_acquire script failed: %w", err)
	}
	
	// Result should be 1 (acquired) or 0 (not acquired)
	acquired, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected lease_acquire result type: %T", result)
	}
	
	return acquired == 1, nil
}

// LeaseRelease executes the lease_release.lua script
func (sm *ScriptManager) LeaseRelease(ctx context.Context, req LeaseReleaseRequest) error {
	keys := []string{req.LeaseKey}
	args := []interface{}{req.OwnerID}
	
	_, err := sm.client.EvalSHA(ctx, sm.leaseReleaseSHA, keys, args)
	if err != nil {
		return fmt.Errorf("lease_release script failed: %w", err)
	}
	
	return nil
}

// DecisionSet executes the decision_set.lua script
func (sm *ScriptManager) DecisionSet(ctx context.Context, req DecisionRequest) error {
	keys := []string{req.DecisionKey}
	args := []interface{}{
		sm.decisionToString(req.Decision),
		req.TTL.Milliseconds(),
		boolToInt(req.Overwrite),
		req.Reason,
	}
	
	_, err := sm.client.EvalSHA(ctx, sm.decisionSetSHA, keys, args)
	if err != nil {
		return fmt.Errorf("decision_set script failed: %w", err)
	}
	
	return nil
}

// CompleteAndGC executes the complete_and_gc.lua script
func (sm *ScriptManager) CompleteAndGC(ctx context.Context, req CompleteRequest) error {
	keys := []string{
		req.Keys.Meta,
		req.Keys.Spans,
		req.Keys.Dedup,
		req.Keys.Segments,
		req.Keys.Decision,
		req.Keys.Lease,
		req.Keys.Active,
	}
	
	args := []interface{}{
		fmt.Sprintf("%x", req.TraceID),
		boolToInt(req.Hard),
		req.MaxKeys,
	}
	
	_, err := sm.client.EvalSHA(ctx, sm.completeAndGCSHA, keys, args)
	if err != nil {
		return fmt.Errorf("complete_and_gc script failed: %w", err)
	}
	
	return nil
}

// Helper methods


func (sm *ScriptManager) parseAppendResponse(result interface{}) (AppendResponse, error) {
	// For now, return a basic response
	// In a full implementation, this would parse the Lua script result JSON
	if result == nil {
		return AppendResponse{}, fmt.Errorf("nil result from append script")
	}
	
	// Placeholder response - would parse actual Redis script result
	return AppendResponse{
		Appended:       1,
		Created:        1,
		OverLimit:      0,
		SpanCount:      1,
		ApproxBytes:    100,
		AlreadyDecided: false,
	}, nil
}

func (sm *ScriptManager) decisionToString(decision store.Decision) string {
	switch decision {
	case store.DecisionSampled:
		return "sampled"
	case store.DecisionNotSampled:
		return "not_sampled"
	default:
		return "unknown"
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// getScript returns the Lua script for the given name
func (sm *ScriptManager) getScript(name string) string {
	switch name {
	case "append_and_index":
		return appendAndIndexScript
	case "lease_acquire":
		return leaseAcquireScript
	case "lease_release":
		return leaseReleaseScript
	case "decision_set":
		return decisionSetScript
	case "spill_add_segment":
		return spillAddSegmentScript
	case "complete_and_gc":
		return completeAndGCScript
	default:
		return ""
	}
}

// Lua scripts (simplified versions based on REDIS_LUA_SCRIPT.md)

const appendAndIndexScript = `
-- append_and_index.lua
local meta    = KEYS[1]
local spans   = KEYS[2]  
local dedup   = KEYS[3]
local lease   = KEYS[4]
local active  = KEYS[5]
local decision= KEYS[6]

local now_ms              = tonumber(ARGV[1])
local ttl_ms              = tonumber(ARGV[2])
local decision_wait_ms    = tonumber(ARGV[3])
local tenant              = ARGV[4]
local trace_member        = ARGV[5]
local span_id             = ARGV[6]
local span_payload        = ARGV[7]
local span_payload_len    = tonumber(ARGV[8])
local max_spans_per_trace = tonumber(ARGV[9])
local max_bytes_per_trace = tonumber(ARGV[10])
local lease_owner         = ARGV[11]
local refresh_lease       = tonumber(ARGV[12])

-- If decided already, we don't append (idempotent no-op)
local d = redis.call('GET', decision)
if d then
  return cjson.encode({appended=0, decided=1})
end

-- Dedup: set span_id if not present
local newspan = redis.call('HSETNX', dedup, span_id, '1')

local first_seen_ms = redis.call('HGET', meta, 'first_seen_ms')
local created = 0
if not first_seen_ms then
  -- First time we see this trace
  first_seen_ms = tostring(now_ms)
  redis.call('HSET', meta,
    'tenant', tenant,
    'first_seen_ms', first_seen_ms,
    'last_seen_ms', tostring(now_ms),
    'span_count', '0',
    'approx_bytes', '0',
    'state', 'active'
  )
  created = 1
  -- index into active set with decision due time
  local due_ms = now_ms + decision_wait_ms
  redis.call('ZADD', active, due_ms, trace_member)
else
  -- Update last_seen
  redis.call('HSET', meta, 'last_seen_ms', tostring(now_ms))
end

local appended = 0
local span_count = tonumber(redis.call('HGET', meta, 'span_count') or '0')
local approx_bytes = tonumber(redis.call('HGET', meta, 'approx_bytes') or '0')

if newspan == 1 then
  redis.call('RPUSH', spans, span_payload)
  span_count = span_count + 1
  approx_bytes = approx_bytes + span_payload_len
  redis.call('HSET', meta, 'span_count', tostring(span_count), 'approx_bytes', tostring(approx_bytes))
  appended = 1
end

-- Guardrails: mark 'spilling' state if over limits
local over_limit = 0
if (max_spans_per_trace > 0 and span_count > max_spans_per_trace) or
   (max_bytes_per_trace > 0 and approx_bytes > max_bytes_per_trace) then
  redis.call('HSET', meta, 'state', 'spilling')
  over_limit = 1
end

-- TTLs on hot keys
redis.call('PEXPIRE', meta, ttl_ms)
redis.call('PEXPIRE', spans, ttl_ms)
redis.call('PEXPIRE', dedup, ttl_ms)

-- Optional lease refresh if caller already owns it
if refresh_lease == 1 and lease_owner and lease_owner ~= '' then
  local cur = redis.call('GET', lease)
  if cur == lease_owner then
    redis.call('PEXPIRE', lease, math.floor(ttl_ms/6))
  end
end

return cjson.encode({
  appended=appended,
  created=created,
  over_limit=over_limit,
  span_count=span_count,
  approx_bytes=approx_bytes
})
`

const leaseAcquireScript = `
-- lease_acquire.lua
local lease_key = KEYS[1]
local owner_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local refresh = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

local current_owner = redis.call('GET', lease_key)

if not current_owner then
  -- No current lease, acquire it
  redis.call('SET', lease_key, owner_id, 'PX', ttl_ms)
  return 1
elseif current_owner == owner_id and refresh == 1 then
  -- Refresh existing lease
  redis.call('PEXPIRE', lease_key, ttl_ms)
  return 1
else
  -- Lease owned by someone else
  return 0
end
`

const leaseReleaseScript = `
-- lease_release.lua
local lease_key = KEYS[1]
local owner_id = ARGV[1]

local current_owner = redis.call('GET', lease_key)

if current_owner == owner_id then
  redis.call('DEL', lease_key)
  return 1
else
  return 0
end
`

const decisionSetScript = `
-- decision_set.lua
local decision_key = KEYS[1]
local decision = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local overwrite = tonumber(ARGV[3])
local reason = ARGV[4]

local current = redis.call('GET', decision_key)

if not current or overwrite == 1 then
  redis.call('SET', decision_key, decision, 'PX', ttl_ms)
  return 1
else
  return 0
end
`

const spillAddSegmentScript = `
-- spill_add_segment.lua
-- Atomically adds a segment reference to Redis trace metadata
-- This script integrates with the object storage spillover system
--
-- KEYS[1]: segments list key (ts:trace:{id}:segments)
-- KEYS[2]: meta hash key (ts:trace:{id}:meta)
--
-- ARGV[1]: segment URL (gs://... or s3://...)
-- ARGV[2]: codec type (avro or jsonl.zst)
-- ARGV[3]: span count in segment
-- ARGV[4]: compressed bytes in segment
-- ARGV[5]: sequence number
-- ARGV[6]: created timestamp (milliseconds)
-- ARGV[7]: etag (optional integrity hash)
-- ARGV[8]: crc32c (optional GCS checksum)

local segments_key = KEYS[1]
local meta_key = KEYS[2]

-- Extract arguments
local url = ARGV[1]
local codec = ARGV[2]
local count = tonumber(ARGV[3]) or 0
local bytes = tonumber(ARGV[4]) or 0
local seq = tonumber(ARGV[5]) or 0
local created_at = tonumber(ARGV[6]) or 0
local etag = ARGV[7] or ""
local crc32c = tonumber(ARGV[8]) or 0

-- Validate required fields
if url == "" or codec == "" then
    return 0
end

-- Create segment reference as JSON-encoded string for easy parsing
local segment_ref = string.format(
    '{"url":"%s","codec":"%s","count":%d,"bytes":%d,"seq":%d,"created_at":%d,"etag":"%s","crc32c":%d}',
    url, codec, count, bytes, seq, created_at, etag, crc32c
)

-- Add segment reference to the segments LIST (newest first)
redis.call("LPUSH", segments_key, segment_ref)

-- Update trace metadata to reflect spill state
local current_segments = tonumber(redis.call("HGET", meta_key, "spill_segments") or "0")
local current_spill_bytes = tonumber(redis.call("HGET", meta_key, "spill_bytes") or "0")

redis.call("HSET", meta_key,
    "spill_segments", current_segments + 1,
    "spill_bytes", current_spill_bytes + bytes,
    "last_spill_at", created_at,
    "state", "spilled"
)

-- Set TTL on spill-related keys (24 hours = 86400000 ms)
-- This ensures spilled data doesn't accumulate indefinitely
redis.call("PEXPIRE", segments_key, 86400000)
redis.call("PEXPIRE", meta_key, 86400000)

-- Return success (number of segments added)
return 1
`

const completeAndGCScript = `
-- complete_and_gc.lua
local meta_key = KEYS[1]
local spans_key = KEYS[2]
local dedup_key = KEYS[3]
local segments_key = KEYS[4]
local decision_key = KEYS[5]
local lease_key = KEYS[6]
local active_key = KEYS[7]

local trace_member = ARGV[1]
local hard = tonumber(ARGV[2])
local max_keys = tonumber(ARGV[3])

-- Remove from active set
redis.call('ZREM', active_key, trace_member)

if hard == 1 then
  -- Hard delete all keys
  redis.call('DEL', meta_key, spans_key, dedup_key, segments_key, decision_key, lease_key)
else
  -- Rely on TTL expiration
  redis.call('EXPIRE', meta_key, 60)
  redis.call('EXPIRE', spans_key, 60)
  redis.call('EXPIRE', dedup_key, 60)
end

return 1
`