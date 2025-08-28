// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// MemStore implements TraceBufferStore using in-memory data structures.
type MemStore struct {
	cfg    store.Config
	now    func() time.Time
	mu     sync.RWMutex
	traces map[store.TraceID]*memTrace
	stats  store.Stats
}

// memTrace holds all data for a single trace.
type memTrace struct {
	meta     store.Meta
	dec      *store.Decision
	lease    lease
	spans    []store.SpanRecord
	segments []store.SpillSegmentRef
	dedupSet map[pcommon.SpanID]bool // span_id deduplication
}

type lease struct {
	owner  string
	expiry time.Time
}

// NewMemStore creates a new in-memory trace buffer store.
func NewMemStore(cfg store.Config) *MemStore {
	return &MemStore{
		cfg:    cfg,
		now:    time.Now,
		traces: make(map[store.TraceID]*memTrace),
		stats:  store.Stats{Errors: make(map[string]int64)},
	}
}

// SetTimeProvider allows overriding time function for testing.
func (s *MemStore) SetTimeProvider(timeFunc func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = timeFunc
}

func (s *MemStore) Append(ctx context.Context, tenant store.Tenant, traceID store.TraceID, spans store.EncodedSpans) (store.AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	trace := s.getOrCreateTrace(traceID, tenant, now)

	// For this implementation, we'll directly work with the span count
	// In a real implementation, we would decode OTLP bytes via the adapter
	var newSpanCount int
	var newBytes int64

	// Generate unique span IDs for testing (spans.Count number of spans)
	// Use current span count as offset to ensure uniqueness across calls
	for i := 0; i < spans.Count; i++ {
		offset := trace.meta.SpanCount + i
		spanIDBytes := [8]byte{byte(offset), byte(offset >> 8), byte(offset >> 16), byte(offset >> 24), 0, 0, 0, 0}
		spanID := pcommon.SpanID(spanIDBytes)

		// Deduplicate by span_id
		if trace.dedupSet[spanID] {
			s.stats.AppendsDeduped++
			continue
		}

		trace.dedupSet[spanID] = true

		// Create placeholder span record
		span := store.SpanRecord{
			TraceID: traceID,
			SpanID:  spanID,
			Name:    "placeholder",
			Raw:     spans.OTLP,
		}
		trace.spans = append(trace.spans, span)
		newSpanCount++
		newBytes += int64(spans.Bytes / spans.Count) // distribute bytes evenly
	}

	// Update metadata
	trace.meta.LastSeen = now
	trace.meta.SpanCount += newSpanCount
	trace.meta.ApproxBytes += newBytes

	// Check limits for spill trigger
	var spilled bool
	if s.cfg.HotLimits.MaxSpanPerTrace > 0 && trace.meta.SpanCount > s.cfg.HotLimits.MaxSpanPerTrace {
		trace.meta.State = store.StateSpilled
		spilled = true
	}
	if s.cfg.HotLimits.MaxBytesPerTrace > 0 && trace.meta.ApproxBytes > s.cfg.HotLimits.MaxBytesPerTrace {
		trace.meta.State = store.StateSpilled
		spilled = true
	}

	s.stats.AppendsTotal++
	s.stats.InFlightTraces = int64(len(s.traces))
	s.updateHotBytes()

	return store.AppendResult{
		NewSpanCount:   newSpanCount,
		NewApproxBytes: newBytes,
		Spilled:        spilled,
	}, nil
}

func (s *MemStore) TryAcquireLease(ctx context.Context, traceID store.TraceID, opts store.LeaseOptions) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	trace := s.traces[traceID]
	if trace == nil {
		return false, store.ErrNotFound
	}

	// Check if lease exists and is valid
	if trace.lease.expiry.After(now) {
		if trace.lease.owner == opts.OwnerID {
			// Refresh lease if requested
			if opts.Refresh {
				trace.lease.expiry = now.Add(opts.TTL)
				trace.meta.LeaseExpiry = trace.lease.expiry
			}
			return true, nil
		}
		// Lease owned by someone else
		s.stats.LeaseConflicts++
		return false, store.ErrLeaseOwned
	}

	// Acquire new lease
	trace.lease = lease{
		owner:  opts.OwnerID,
		expiry: now.Add(opts.TTL),
	}
	trace.meta.Owner = opts.OwnerID
	trace.meta.LeaseExpiry = trace.lease.expiry

	s.stats.LeasesGranted++
	return true, nil
}

func (s *MemStore) ReleaseLease(ctx context.Context, traceID store.TraceID, ownerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trace := s.traces[traceID]
	if trace == nil {
		return store.ErrNotFound
	}

	if trace.lease.owner != ownerID {
		return store.ErrLeaseMismatch
	}

	// Clear lease
	trace.lease = lease{}
	trace.meta.Owner = ""
	trace.meta.LeaseExpiry = time.Time{}

	return nil
}

func (s *MemStore) FetchMeta(ctx context.Context, traceID store.TraceID) (store.Meta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trace := s.traces[traceID]
	if trace == nil {
		return store.Meta{}, store.ErrNotFound
	}

	return trace.meta, nil
}

func (s *MemStore) FetchAll(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.DecodedSpans, store.Meta, error) {
	if opts.Streaming {
		return nil, store.Meta{}, store.ErrUseIterator
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	trace := s.traces[traceID]
	if trace == nil {
		return nil, store.Meta{}, store.ErrNotFound
	}

	decoded := &memDecodedSpans{spans: trace.spans}
	return decoded, trace.meta, nil
}

func (s *MemStore) OpenIterator(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.SpanIterator, store.Meta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trace := s.traces[traceID]
	if trace == nil {
		return nil, store.Meta{}, store.ErrNotFound
	}

	batchSize := 1000 // default batch size
	if opts.MaxSpans > 0 && opts.MaxSpans < batchSize {
		batchSize = opts.MaxSpans
	}

	iter := &memSpanIterator{
		spans:     trace.spans,
		batchSize: batchSize,
		pos:       0,
	}

	return iter, trace.meta, nil
}

func (s *MemStore) SetDecision(ctx context.Context, traceID store.TraceID, decision store.Decision, opts store.DecisionOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trace := s.traces[traceID]
	if trace == nil {
		return store.ErrNotFound
	}

	// Check if decision already set and overwrite not allowed
	if trace.dec != nil && !opts.Overwrite {
		if *trace.dec == decision {
			return nil // idempotent
		}
		return store.ErrContext // decision conflict
	}

	trace.dec = &decision
	trace.meta.State = store.StateDecided

	// Update stats
	switch decision {
	case store.DecisionSampled:
		s.stats.DecisionsSampled++
	case store.DecisionNotSampled:
		s.stats.DecisionsNotSampled++
	}

	return nil
}

func (s *MemStore) GetDecision(ctx context.Context, traceID store.TraceID) (store.Decision, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trace := s.traces[traceID]
	if trace == nil {
		return store.DecisionUnknown, false, store.ErrNotFound
	}

	if trace.dec == nil {
		return store.DecisionUnknown, false, nil
	}

	s.stats.DecisionsCachedHits++
	return *trace.dec, true, nil
}

func (s *MemStore) MarkComplete(ctx context.Context, traceID store.TraceID, gc store.GCOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trace := s.traces[traceID]
	if trace == nil {
		return store.ErrNotFound
	}

	if gc.Hard {
		delete(s.traces, traceID)
	} else {
		trace.meta.State = store.StateCompleted
	}

	s.stats.InFlightTraces = int64(len(s.traces))
	s.updateHotBytes()

	return nil
}

func (s *MemStore) SpillNow(ctx context.Context, traceID store.TraceID) (store.SpillSegmentRef, error) {
	// Memory store doesn't support actual spilling to object storage
	// This is a no-op for memory implementation
	return store.SpillSegmentRef{}, store.ErrSpillDown
}

func (s *MemStore) Rehydrate(ctx context.Context, traceID store.TraceID) error {
	// Memory store doesn't support rehydration since there's no spill
	return nil
}

func (s *MemStore) Stats(ctx context.Context) (store.Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy of stats
	statsCopy := s.stats
	statsCopy.Errors = make(map[string]int64)
	for k, v := range s.stats.Errors {
		statsCopy.Errors[k] = v
	}

	return statsCopy, nil
}

func (s *MemStore) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.traces = make(map[store.TraceID]*memTrace)
	s.stats = store.Stats{Errors: make(map[string]int64)}

	return nil
}

// Helper methods

func (s *MemStore) getOrCreateTrace(traceID store.TraceID, tenant store.Tenant, now time.Time) *memTrace {
	trace := s.traces[traceID]
	if trace == nil {
		trace = &memTrace{
			meta: store.Meta{
				Tenant:    tenant,
				TraceID:   traceID,
				FirstSeen: now,
				LastSeen:  now,
				State:     store.StateActive,
				Custom:    make(map[string]string),
			},
			spans:    make([]store.SpanRecord, 0),
			segments: make([]store.SpillSegmentRef, 0),
			dedupSet: make(map[pcommon.SpanID]bool),
		}
		s.traces[traceID] = trace
	}
	return trace
}

func (s *MemStore) updateHotBytes() {
	var total int64
	for _, trace := range s.traces {
		total += trace.meta.ApproxBytes
	}
	s.stats.HotBytes = total
}

func (s *MemStore) decodeSpans(spans store.EncodedSpans) (store.DecodedSpans, error) {
	// For now, we'll implement a simple decoder that expects spans already in native format
	// This will be enhanced when we implement the adapter layer integration
	if spans.Count == 0 {
		return &memDecodedSpans{spans: []store.SpanRecord{}}, nil
	}

	// For memory store, we'll create placeholder spans to maintain interface compatibility
	// Real implementation will use adapter layer to decode OTLP bytes
	placeholderSpans := make([]store.SpanRecord, spans.Count)
	for i := 0; i < spans.Count; i++ {
		placeholderSpans[i] = store.SpanRecord{
			// Minimal placeholder data - will be populated by adapter
			Name: "placeholder",
			Raw:  spans.OTLP,
		}
	}

	return &memDecodedSpans{spans: placeholderSpans}, nil
}
