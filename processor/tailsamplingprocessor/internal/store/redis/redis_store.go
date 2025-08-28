// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// SpillManager represents the interface for spill operations (avoiding import cycles)
type SpillManager interface {
	// SpillTrace writes trace spans to object storage and returns segment reference
	SpillTrace(ctx context.Context, traceID [16]byte, tenant string, spans store.DecodedSpans) (SpillSegmentRef, error)
}

// SpillSegmentRef represents a spilled segment reference (avoiding import cycles)
type SpillSegmentRef struct {
	URL        string
	Codec      int8 // SpillCodec
	FirstIndex int
	Count      int
	Bytes      int64
	CreatedAt  time.Time
}

// RedisStore implements the TraceBufferStore interface using Redis as the backend
// It follows the Redis data model and Lua scripts defined in REDIS_LUA_SCRIPT.md
type RedisStore struct {
	logger   *zap.Logger
	client   RedisClient
	scripts  *ScriptManager
	config   Config
	keyspace string
	ownerID  string

	// Hot limits for per-trace guardrails
	hotLimits store.Config

	// Spill manager for object storage operations (optional)
	spillManager SpillManager

	// Metrics and stats
	stats StoreStats
}

// NewRedisStore creates a new Redis-backed TraceBufferStore implementation
func NewRedisStore(ctx context.Context, logger *zap.Logger, config Config, hotLimits store.Config) (*RedisStore, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Create Redis client with connection pooling
	client, err := NewRedisClient(config.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}

	// Test connection
	if err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Initialize Lua script manager
	scripts := NewScriptManager(client, logger)
	if err := scripts.LoadScripts(ctx); err != nil {
		return nil, fmt.Errorf("failed to load Lua scripts: %w", err)
	}

	// Generate unique owner ID for this process (hostname+pid)
	ownerID, err := generateOwnerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate owner ID: %w", err)
	}

	rs := &RedisStore{
		logger:    logger.With(zap.String("component", "redis-store")),
		client:    client,
		scripts:   scripts,
		config:    config,
		keyspace:  config.Keyspace,
		ownerID:   ownerID,
		hotLimits: hotLimits,
		stats:     StoreStats{},
	}

	logger.Info("Redis store initialized",
		zap.String("keyspace", config.Keyspace),
		zap.String("owner_id", ownerID),
		zap.Int("max_spans_per_trace", hotLimits.HotLimits.MaxSpanPerTrace),
		zap.Int64("max_bytes_per_trace", hotLimits.HotLimits.MaxBytesPerTrace))

	return rs, nil
}

// Append implements store.TraceBufferStore
func (rs *RedisStore) Append(ctx context.Context, tenant store.Tenant, traceID store.TraceID, spans store.EncodedSpans) (store.AppendResult, error) {
	// Convert spans to Redis format and execute append_and_index.lua script
	// This will be implemented using the Lua script from REDIS_LUA_SCRIPT.md

	rs.stats.AppendCalls++

	// Generate Redis keys for this trace
	keys := rs.generateTraceKeys(traceID)

	// For simplicity, append each span individually
	// In a full implementation, we would extract individual spans from the batch
	var payload []byte
	var payloadLen int
	if spans.OTLP != nil {
		payload = spans.OTLP
		payloadLen = spans.Bytes
	} else if spans.Native != nil {
		payload = spans.Native
		payloadLen = spans.Bytes
	}

	// Generate a span ID for deduplication (simplified)
	spanID := fmt.Sprintf("span_%d", len(payload))

	// Execute append_and_index.lua script atomically
	result, err := rs.scripts.AppendAndIndex(ctx, AppendRequest{
		Keys:             keys,
		Tenant:           string(tenant),
		TraceID:          traceID,
		SpanID:           spanID,
		SpanPayload:      payload,
		SpanPayloadLen:   payloadLen,
		TTL:              rs.config.DefaultTTL,
		DecisionWait:     rs.config.DecisionWait,
		MaxSpansPerTrace: rs.hotLimits.HotLimits.MaxSpanPerTrace,
		MaxBytesPerTrace: rs.hotLimits.HotLimits.MaxBytesPerTrace,
		LeaseOwner:       rs.ownerID,
		RefreshLease:     true,
	})

	if err != nil {
		rs.stats.AppendErrors++
		return store.AppendResult{}, fmt.Errorf("append operation failed: %w", err)
	}

	return store.AppendResult{
		NewSpanCount:   result.Appended,
		NewApproxBytes: result.ApproxBytes,
		Spilled:        result.OverLimit > 0,
	}, nil
}

// TryAcquireLease implements store.TraceBufferStore
func (rs *RedisStore) TryAcquireLease(ctx context.Context, traceID store.TraceID, opts store.LeaseOptions) (bool, error) {
	rs.stats.LeaseCalls++

	keys := rs.generateTraceKeys(traceID)

	acquired, err := rs.scripts.LeaseAcquire(ctx, LeaseRequest{
		LeaseKey: keys.Lease,
		OwnerID:  opts.OwnerID,
		TTL:      opts.TTL,
		Refresh:  opts.Refresh,
	})

	if err != nil {
		rs.stats.LeaseErrors++
		return false, fmt.Errorf("lease acquisition failed: %w", err)
	}

	if acquired {
		rs.stats.LeasesAcquired++
	} else {
		rs.stats.LeaseConflicts++
	}

	return acquired, nil
}

// ReleaseLease implements store.TraceBufferStore
func (rs *RedisStore) ReleaseLease(ctx context.Context, traceID store.TraceID, ownerID string) error {
	rs.stats.LeaseReleaseCalls++

	keys := rs.generateTraceKeys(traceID)

	err := rs.scripts.LeaseRelease(ctx, LeaseReleaseRequest{
		LeaseKey: keys.Lease,
		OwnerID:  ownerID,
	})

	if err != nil {
		rs.stats.LeaseReleaseErrors++
		return fmt.Errorf("lease release failed: %w", err)
	}

	rs.stats.LeasesReleased++
	return nil
}

// FetchMeta implements store.TraceBufferStore
func (rs *RedisStore) FetchMeta(ctx context.Context, traceID store.TraceID) (store.Meta, error) {
	rs.stats.MetaFetchCalls++

	keys := rs.generateTraceKeys(traceID)

	meta, err := rs.fetchTraceMetadata(ctx, keys)
	if err != nil {
		rs.stats.MetaFetchErrors++
		return store.Meta{}, err
	}

	return meta, nil
}

// FetchAll implements store.TraceBufferStore
func (rs *RedisStore) FetchAll(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.DecodedSpans, store.Meta, error) {
	rs.stats.FetchAllCalls++

	keys := rs.generateTraceKeys(traceID)

	// Fetch metadata first
	meta, err := rs.fetchTraceMetadata(ctx, keys)
	if err != nil {
		rs.stats.FetchAllErrors++
		return nil, store.Meta{}, err
	}

	// Fetch all spans
	spans, err := rs.fetchAllSpans(ctx, keys, opts)
	if err != nil {
		rs.stats.FetchAllErrors++
		return nil, store.Meta{}, err
	}

	return spans, meta, nil
}

// OpenIterator implements store.TraceBufferStore
func (rs *RedisStore) OpenIterator(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.SpanIterator, store.Meta, error) {
	rs.stats.IteratorCalls++

	keys := rs.generateTraceKeys(traceID)

	// Fetch metadata first
	meta, err := rs.fetchTraceMetadata(ctx, keys)
	if err != nil {
		rs.stats.IteratorErrors++
		return nil, store.Meta{}, err
	}

	// Create streaming iterator
	iterator, err := NewRedisSpanIterator(ctx, rs.client, keys, opts, rs.logger)
	if err != nil {
		rs.stats.IteratorErrors++
		return nil, store.Meta{}, err
	}

	return iterator, meta, nil
}

// SetDecision implements store.TraceBufferStore
func (rs *RedisStore) SetDecision(ctx context.Context, traceID store.TraceID, decision store.Decision, opts store.DecisionOptions) error {
	rs.stats.DecisionSetCalls++

	keys := rs.generateTraceKeys(traceID)

	err := rs.scripts.DecisionSet(ctx, DecisionRequest{
		DecisionKey: keys.Decision,
		Decision:    decision,
		TTL:         opts.TTL,
		Overwrite:   opts.Overwrite,
		Reason:      opts.Reason,
	})

	if err != nil {
		rs.stats.DecisionSetErrors++
		return fmt.Errorf("decision set failed: %w", err)
	}

	return nil
}

// GetDecision implements store.TraceBufferStore
func (rs *RedisStore) GetDecision(ctx context.Context, traceID store.TraceID) (store.Decision, bool, error) {
	rs.stats.DecisionGetCalls++

	keys := rs.generateTraceKeys(traceID)

	decision, found, err := rs.getDecisionFromRedis(ctx, keys.Decision)
	if err != nil {
		rs.stats.DecisionGetErrors++
		return store.DecisionUnknown, false, err
	}

	return decision, found, nil
}

// MarkComplete implements store.TraceBufferStore
func (rs *RedisStore) MarkComplete(ctx context.Context, traceID store.TraceID, gc store.GCOptions) error {
	rs.stats.CompleteCalls++

	keys := rs.generateTraceKeys(traceID)

	err := rs.scripts.CompleteAndGC(ctx, CompleteRequest{
		Keys:    keys,
		TraceID: traceID,
		Hard:    gc.Hard,
		MaxKeys: gc.MaxKeys,
	})

	if err != nil {
		rs.stats.CompleteErrors++
		return fmt.Errorf("mark complete failed: %w", err)
	}

	return nil
}

// SpillNow implements store.TraceBufferStore
func (rs *RedisStore) SpillNow(ctx context.Context, traceID store.TraceID) (store.SpillSegmentRef, error) {
	rs.stats.SpillCalls++

	// Check if spill is enabled in configuration
	if rs.spillManager == nil {
		rs.stats.SpillErrors++
		return store.SpillSegmentRef{}, fmt.Errorf("spill is not configured")
	}

	// Fetch the trace data for spilling
	spans, meta, err := rs.FetchAll(ctx, traceID, store.FetchOptions{})
	if err != nil {
		rs.stats.SpillErrors++
		return store.SpillSegmentRef{}, fmt.Errorf("failed to fetch trace for spill: %w", err)
	}

	// Convert store types to spill types for the spill manager
	spillTraceID := convertToSpillTraceID(traceID)
	spillTenant := convertToSpillTenant(meta.Tenant)

	// Use the spill manager to write to object storage
	spillRef, err := rs.spillManager.SpillTrace(ctx, spillTraceID, spillTenant, spans)
	if err != nil {
		rs.stats.SpillErrors++
		return store.SpillSegmentRef{}, fmt.Errorf("failed to spill trace to object storage: %w", err)
	}

	// Add segment reference to Redis using spill_add_segment.lua
	err = rs.indexSpillSegment(ctx, traceID, spillRef)
	if err != nil {
		rs.stats.SpillErrors++
		return store.SpillSegmentRef{}, fmt.Errorf("failed to index spill segment in Redis: %w", err)
	}

	// Convert spill segment reference back to store format
	storeRef := store.SpillSegmentRef{
		URL:        spillRef.URL,
		Codec:      convertFromSpillCodec(spillRef.Codec),
		FirstIndex: spillRef.FirstIndex,
		Count:      spillRef.Count,
		Bytes:      spillRef.Bytes,
		CreatedAt:  spillRef.CreatedAt,
	}

	return storeRef, nil
}

// Rehydrate implements store.TraceBufferStore
func (rs *RedisStore) Rehydrate(ctx context.Context, traceID store.TraceID) error {
	rs.stats.RehydrateCalls++

	// Rehydrate implementation will be added in Phase 3
	rs.stats.RehydrateErrors++
	return fmt.Errorf("rehydrate functionality not yet implemented")
}

// Stats implements store.TraceBufferStore
func (rs *RedisStore) Stats(ctx context.Context) (store.Stats, error) {
	// Convert internal stats to store.Stats format
	errors := make(map[string]int64)
	errors["append"] = rs.stats.AppendErrors
	errors["fetch"] = rs.stats.FetchAllErrors
	errors["lease"] = rs.stats.LeaseErrors
	errors["decision"] = rs.stats.DecisionSetErrors + rs.stats.DecisionGetErrors
	
	return store.Stats{
		InFlightTraces:      rs.stats.TracesActive,
		HotBytes:            rs.stats.BytesBuffered,
		SpillSegments:       0, // Will be implemented in Phase 3
		SpillBytes:          0, // Will be implemented in Phase 3
		AppendsTotal:        rs.stats.AppendCalls,
		AppendsDeduped:      0, // Would need tracking in Lua scripts
		LeasesGranted:       rs.stats.LeasesAcquired,
		LeaseConflicts:      rs.stats.LeaseConflicts,
		DecisionsSampled:    0, // Would need tracking by decision type
		DecisionsNotSampled: 0, // Would need tracking by decision type
		DecisionsCachedHits: 0, // Would need cache hit tracking
		LateSpans:           0, // Would need tracking in Lua scripts
		Errors:              errors,
	}, nil
}

// Close implements store.TraceBufferStore
func (rs *RedisStore) Close(ctx context.Context) error {
	rs.logger.Info("Closing Redis store")

	if rs.client != nil {
		return rs.client.Close()
	}

	return nil
}

// Helper methods

func (rs *RedisStore) generateTraceKeys(traceID store.TraceID) TraceKeys {
	traceIDHex := traceID.String()
	prefix := rs.keyspace + "trace:" + traceIDHex

	return TraceKeys{
		Meta:     prefix + ":meta",
		Spans:    prefix + ":spans",
		Dedup:    prefix + ":dedup",
		Segments: prefix + ":segments",
		Decision: prefix + ":decision",
		Lease:    prefix + ":lease",
		Active:   rs.keyspace + "active", // Global active traces ZSET
	}
}

func (rs *RedisStore) calculateErrorRate() float64 {
	totalOps := rs.stats.AppendCalls + rs.stats.FetchAllCalls + rs.stats.LeaseCalls + rs.stats.DecisionSetCalls + rs.stats.DecisionGetCalls
	totalErrors := rs.stats.AppendErrors + rs.stats.FetchAllErrors + rs.stats.LeaseErrors + rs.stats.DecisionSetErrors + rs.stats.DecisionGetErrors

	if totalOps == 0 {
		return 0.0
	}

	return float64(totalErrors) / float64(totalOps)
}

// fetchTraceMetadata retrieves metadata from Redis HASH
func (rs *RedisStore) fetchTraceMetadata(ctx context.Context, keys TraceKeys) (store.Meta, error) {
	metaData, err := rs.client.HGetAll(ctx, keys.Meta)
	if err != nil {
		return store.Meta{}, fmt.Errorf("failed to fetch trace metadata: %w", err)
	}

	if len(metaData) == 0 {
		return store.Meta{}, store.ErrNotFound
	}

	// Parse metadata fields from Redis HASH
	meta := store.Meta{}
	if spanCountStr, exists := metaData["span_count"]; exists {
		if spanCount, err := parseInt64(spanCountStr); err == nil {
			meta.SpanCount = int(spanCount)
		}
	}
	if approxBytesStr, exists := metaData["approx_bytes"]; exists {
		if approxBytes, err := parseInt64(approxBytesStr); err == nil {
			meta.ApproxBytes = approxBytes
		}
	}
	if firstSeenStr, exists := metaData["first_seen"]; exists {
		if firstSeen, err := parseTime(firstSeenStr); err == nil {
			meta.FirstSeen = firstSeen
		}
	}
	if lastSeenStr, exists := metaData["last_seen"]; exists {
		if lastSeen, err := parseTime(lastSeenStr); err == nil {
			meta.LastSeen = lastSeen
		}
	}

	return meta, nil
}

// fetchAllSpans retrieves all spans for a trace
func (rs *RedisStore) fetchAllSpans(ctx context.Context, keys TraceKeys, opts store.FetchOptions) (store.DecodedSpans, error) {
	// Fetch span count to determine range
	spanCount, err := rs.client.LLen(ctx, keys.Spans)
	if err != nil {
		return nil, fmt.Errorf("failed to get span count: %w", err)
	}

	if spanCount == 0 {
		return NewEmptyDecodedSpans(), nil
	}

	// Calculate fetch range based on options
	start := int64(0)
	stop := spanCount - 1
	if opts.MaxSpans > 0 && opts.MaxSpans < int(spanCount) {
		stop = int64(opts.MaxSpans) - 1
	}

	// Fetch spans from Redis LIST
	spanData, err := rs.client.LRange(ctx, keys.Spans, start, stop)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch spans: %w", err)
	}

	// Create decoded spans implementation
	return NewRedisDecodedSpans(spanData), nil
}

// getDecisionFromRedis retrieves decision from Redis
func (rs *RedisStore) getDecisionFromRedis(ctx context.Context, decisionKey string) (store.Decision, bool, error) {
	decisionStr, err := rs.client.Get(ctx, decisionKey)
	if err != nil {
		// Check if key doesn't exist
		if isRedisNil(err) {
			return store.DecisionUnknown, false, nil
		}
		return store.DecisionUnknown, false, fmt.Errorf("failed to get decision: %w", err)
	}

	// Parse decision string
	decision, err := parseDecision(decisionStr)
	if err != nil {
		return store.DecisionUnknown, false, fmt.Errorf("invalid decision format: %w", err)
	}

	return decision, true, nil
}

// Utility functions

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func parseTime(s string) (time.Time, error) {
	timestamp, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(timestamp, 0), nil
}

func parseDecision(s string) (store.Decision, error) {
	switch strings.ToLower(s) {
	case "sampled":
		return store.DecisionSampled, nil
	case "not_sampled":
		return store.DecisionNotSampled, nil
	default:
		return store.DecisionUnknown, fmt.Errorf("unknown decision: %s", s)
	}
}

func isRedisNil(err error) bool {
	return err == redis.Nil
}

// SetSpillManager sets the spill manager for object storage operations
func (rs *RedisStore) SetSpillManager(spillManager SpillManager) {
	rs.spillManager = spillManager
}

// indexSpillSegment adds a segment reference to Redis using spill_add_segment.lua
func (rs *RedisStore) indexSpillSegment(ctx context.Context, traceID store.TraceID, spillRef SpillSegmentRef) error {
	if rs.scripts.spillAddSegmentSHA == "" {
		return fmt.Errorf("spill_add_segment script not loaded")
	}

	keys := rs.generateTraceKeys(traceID)
	
	// Prepare script arguments
	args := []any{
		spillRef.URL,                    // segment URL
		codecToString(spillRef.Codec),   // codec type
		spillRef.Count,                  // span count
		spillRef.Bytes,                  // compressed bytes
		0,                               // sequence number (simplified)
		spillRef.CreatedAt.UnixMilli(),  // created timestamp in milliseconds
		"",                              // etag (not available from simplified reference)
		0,                               // crc32c (not available)
	}

	// Execute the spill_add_segment.lua script
	_, err := rs.client.EvalSHA(ctx, rs.scripts.spillAddSegmentSHA, []string{keys.Segments, keys.Meta}, args)
	if err != nil {
		return fmt.Errorf("failed to execute spill_add_segment script for trace %x: %w", traceID, err)
	}

	return nil
}

// Type conversion functions to avoid import cycles

func convertToSpillTraceID(traceID store.TraceID) [16]byte {
	return [16]byte(traceID)
}

func convertToSpillTenant(tenant store.Tenant) string {
	return string(tenant)
}

func convertFromSpillCodec(codec int8) store.SpillCodec {
	switch codec {
	case 0: // SpillCodecAvro
		return store.SpillCodecAvro
	case 1: // SpillCodecJSONLZstd
		return store.SpillCodecJSONLZstd
	default:
		return store.SpillCodecAvro // default fallback
	}
}

func codecToString(codec int8) string {
	switch codec {
	case 0: // SpillCodecAvro
		return "avro"
	case 1: // SpillCodecJSONLZstd
		return "jsonl.zst"
	default:
		return "avro"
	}
}
