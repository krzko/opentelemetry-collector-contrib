// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/adapter"
)

// InMemoryStore wraps the existing sync.Map-based trace storage to provide
// the TraceBufferStore interface while maintaining 100% backward compatibility.
// This is the default implementation when no storage configuration is provided.
type InMemoryStore struct {
	// idToTrace maintains the existing sync.Map semantics exactly
	idToTrace sync.Map
	
	// numTracesOnMap tracks the number of active traces (same as processor)
	numTracesOnMap *atomic.Uint64
	
	// dropTraceFn is called when traces are dropped (same callback as processor)
	dropTraceFn func(pcommon.TraceID, time.Time)
	
	// adapter for converting between ptrace and store types
	adapter *adapter.PtraceAdapter
}

// NewInMemoryStore creates a new in-memory store that maintains existing behavior
func NewInMemoryStore(dropTraceFn func(pcommon.TraceID, time.Time)) *InMemoryStore {
	return &InMemoryStore{
		numTracesOnMap: &atomic.Uint64{},
		dropTraceFn:    dropTraceFn,
		adapter:        adapter.NewPtraceAdapter(),
	}
}

// Append implements store.TraceBufferStore by maintaining exact sync.Map semantics
func (s *InMemoryStore) Append(ctx context.Context, tenant store.Tenant, traceID store.TraceID, spans store.EncodedSpans) (store.AppendResult, error) {
	// Convert from store.TraceID to pcommon.TraceID for compatibility
	ptraceID := pcommon.TraceID(traceID)
	
	// Decode spans back to ptrace.Traces to maintain existing behavior
	traces, err := s.decodeSpans(spans)
	if err != nil {
		return store.AppendResult{}, err
	}
	
	// Replicate the exact logic from processor.go:processTraces
	d, loaded := s.idToTrace.Load(ptraceID)
	if !loaded {
		spanCount := &atomic.Int64{}
		spanCount.Store(int64(spans.Count))
		
		td := &sampling.TraceData{
			ArrivalTime:     time.Now(),
			SpanCount:       spanCount,
			ReceivedBatches: ptrace.NewTraces(),
		}
		
		if d, loaded = s.idToTrace.LoadOrStore(ptraceID, td); !loaded {
			s.numTracesOnMap.Add(1)
		}
	}
	
	actualData := d.(*sampling.TraceData)
	if loaded {
		actualData.SpanCount.Add(int64(spans.Count))
	}
	
	// Add spans to ReceivedBatches under lock (same as processor)
	actualData.Lock()
	finalDecision := actualData.FinalDecision
	if finalDecision == sampling.Unspecified {
		// Append traces to existing batches
		s.appendTraces(actualData.ReceivedBatches, traces)
	}
	actualData.Unlock()
	
	return store.AppendResult{
		NewSpanCount:   spans.Count,
		NewApproxBytes: int64(spans.Bytes),
		Spilled:        false, // Legacy store never spills
	}, nil
}

// TryAcquireLease implements a no-op lease for legacy store (single instance)
func (s *InMemoryStore) TryAcquireLease(ctx context.Context, traceID store.TraceID, opts store.LeaseOptions) (bool, error) {
	// Legacy store always grants lease (single instance, no coordination needed)
	return true, nil
}

// ReleaseLease implements a no-op for legacy store
func (s *InMemoryStore) ReleaseLease(ctx context.Context, traceID store.TraceID, ownerID string) error {
	// No-op for legacy store
	return nil
}

// FetchMeta returns metadata for a trace
func (s *InMemoryStore) FetchMeta(ctx context.Context, traceID store.TraceID) (store.Meta, error) {
	ptraceID := pcommon.TraceID(traceID)
	
	d, ok := s.idToTrace.Load(ptraceID)
	if !ok {
		return store.Meta{}, store.ErrNotFound
	}
	
	traceData := d.(*sampling.TraceData)
	traceData.Lock()
	defer traceData.Unlock()
	
	return store.Meta{
		TraceID:     traceID,
		FirstSeen:   traceData.ArrivalTime,
		LastSeen:    traceData.ArrivalTime, // Legacy doesn't track last seen separately
		SpanCount:   int(traceData.SpanCount.Load()),
		ApproxBytes: s.calculateApproxBytes(traceData.ReceivedBatches),
		State:       store.StateActive,
		HasSpill:    false,
	}, nil
}

// FetchAll returns all trace data for policy evaluation
func (s *InMemoryStore) FetchAll(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.DecodedSpans, store.Meta, error) {
	if opts.Streaming {
		return nil, store.Meta{}, store.ErrUseIterator
	}
	
	ptraceID := pcommon.TraceID(traceID)
	
	d, ok := s.idToTrace.Load(ptraceID)
	if !ok {
		return nil, store.Meta{}, store.ErrNotFound
	}
	
	traceData := d.(*sampling.TraceData)
	traceData.Lock()
	defer traceData.Unlock()
	
	// Create a copy of the traces for evaluation
	tracesCopy := ptrace.NewTraces()
	traceData.ReceivedBatches.CopyTo(tracesCopy)
	
	meta := store.Meta{
		TraceID:     traceID,
		FirstSeen:   traceData.ArrivalTime,
		LastSeen:    traceData.ArrivalTime,
		SpanCount:   int(traceData.SpanCount.Load()),
		ApproxBytes: s.calculateApproxBytes(traceData.ReceivedBatches),
		State:       store.StateActive,
		HasSpill:    false,
	}
	
	// Convert ptrace.Traces to DecodedSpans
	decodedSpans := &legacyDecodedSpans{
		traces:  tracesCopy,
		adapter: s.adapter,
	}
	
	return decodedSpans, meta, nil
}

// OpenIterator creates an iterator for streaming evaluation
func (s *InMemoryStore) OpenIterator(ctx context.Context, traceID store.TraceID, opts store.FetchOptions) (store.SpanIterator, store.Meta, error) {
	// Get the trace data first
	decodedSpans, meta, err := s.FetchAll(ctx, traceID, store.FetchOptions{Streaming: false})
	if err != nil {
		return nil, store.Meta{}, err
	}
	
	// Create iterator from the decoded spans
	iterator := &legacySpanIterator{
		spans:    decodedSpans,
		returned: false,
	}
	
	return iterator, meta, nil
}

// SetDecision stores the final sampling decision (updates TraceData.FinalDecision)
func (s *InMemoryStore) SetDecision(ctx context.Context, traceID store.TraceID, decision store.Decision, opts store.DecisionOptions) error {
	ptraceID := pcommon.TraceID(traceID)
	
	d, ok := s.idToTrace.Load(ptraceID)
	if !ok {
		return store.ErrNotFound
	}
	
	traceData := d.(*sampling.TraceData)
	traceData.Lock()
	defer traceData.Unlock()
	
	// Convert store.Decision to sampling.Decision
	var samplingDecision sampling.Decision
	switch decision {
	case store.DecisionSampled:
		samplingDecision = sampling.Sampled
	case store.DecisionNotSampled:
		samplingDecision = sampling.NotSampled
	default:
		samplingDecision = sampling.NotSampled
	}
	
	traceData.FinalDecision = samplingDecision
	traceData.DecisionTime = time.Now()
	
	return nil
}

// GetDecision retrieves a cached decision (checks TraceData.FinalDecision)
func (s *InMemoryStore) GetDecision(ctx context.Context, traceID store.TraceID) (store.Decision, bool, error) {
	ptraceID := pcommon.TraceID(traceID)
	
	d, ok := s.idToTrace.Load(ptraceID)
	if !ok {
		return store.DecisionUnknown, false, nil
	}
	
	traceData := d.(*sampling.TraceData)
	traceData.Lock()
	defer traceData.Unlock()
	
	if traceData.FinalDecision == sampling.Unspecified {
		return store.DecisionUnknown, false, nil
	}
	
	// Convert sampling.Decision to store.Decision
	var storeDecision store.Decision
	switch traceData.FinalDecision {
	case sampling.Sampled:
		storeDecision = store.DecisionSampled
	case sampling.NotSampled, sampling.Dropped:
		storeDecision = store.DecisionNotSampled
	default:
		storeDecision = store.DecisionUnknown
	}
	
	return storeDecision, true, nil
}

// MarkComplete removes the trace from storage (same as dropTrace)
func (s *InMemoryStore) MarkComplete(ctx context.Context, traceID store.TraceID, gc store.GCOptions) error {
	ptraceID := pcommon.TraceID(traceID)
	
	// Remove from map
	if _, ok := s.idToTrace.LoadAndDelete(ptraceID); ok {
		// Subtract one from numTracesOnMap
		s.numTracesOnMap.Add(^uint64(0))
		
		// Call drop function if provided
		if s.dropTraceFn != nil {
			s.dropTraceFn(ptraceID, time.Now())
		}
	}
	
	return nil
}

// SpillNow is not supported in legacy store
func (s *InMemoryStore) SpillNow(ctx context.Context, traceID store.TraceID) (store.SpillSegmentRef, error) {
	return store.SpillSegmentRef{}, store.ErrSpillDown
}

// Rehydrate is not supported in legacy store
func (s *InMemoryStore) Rehydrate(ctx context.Context, traceID store.TraceID) error {
	return nil // No-op for legacy store
}

// Stats returns current storage statistics
func (s *InMemoryStore) Stats(ctx context.Context) (store.Stats, error) {
	return store.Stats{
		InFlightTraces: int64(s.numTracesOnMap.Load()),
		HotBytes:       0, // Would need to calculate if needed
	}, nil
}

// Close cleans up resources (no-op for legacy store)
func (s *InMemoryStore) Close(ctx context.Context) error {
	return nil
}

// GetTraceData provides direct access to sampling.TraceData for existing processor logic
// This method is specific to LegacyTraceStore and maintains compatibility
func (s *InMemoryStore) GetTraceData(traceID pcommon.TraceID) (*sampling.TraceData, bool) {
	d, ok := s.idToTrace.Load(traceID)
	if !ok {
		return nil, false
	}
	return d.(*sampling.TraceData), true
}

// LoadOrStoreTraceData provides direct access for existing processor patterns
func (s *InMemoryStore) LoadOrStoreTraceData(traceID pcommon.TraceID, data *sampling.TraceData) (*sampling.TraceData, bool) {
	d, loaded := s.idToTrace.LoadOrStore(traceID, data)
	if !loaded {
		s.numTracesOnMap.Add(1)
	}
	return d.(*sampling.TraceData), loaded
}

// AppendPtraceData appends ptrace.Traces to a trace using the Append() interface
// This method bridges the gap between the processor's ptrace.Traces and the TraceBufferStore interface
func (s *InMemoryStore) AppendPtraceData(ctx context.Context, traceID pcommon.TraceID, traces ptrace.Traces) error {
	// Convert ptrace.Traces to EncodedSpans using adapter
	encoded, err := s.adapter.EncodePtraceToSpans(traces)
	if err != nil {
		return err
	}
	
	// Convert pcommon.TraceID to store.TraceID
	storeTraceID := store.TraceID(traceID)
	
	// Use empty tenant for backward compatibility (existing processor doesn't use tenants)
	tenant := store.Tenant("")
	
	// Call the Append method which maintains existing sync.Map semantics
	_, err = s.Append(ctx, tenant, storeTraceID, encoded)
	return err
}

// Helper methods

func (s *InMemoryStore) decodeSpans(encoded store.EncodedSpans) (ptrace.Traces, error) {
	if len(encoded.OTLP) > 0 {
		// Use existing adapter to decode spans to records, then convert back to traces
		records, err := s.adapter.DecodeSpansToRecords(encoded)
		if err != nil {
			return ptrace.NewTraces(), err
		}
		
		// Convert records back to traces using existing adapter
		traces, err := s.adapter.ConvertRecordsToSpans(records)
		if err != nil {
			return ptrace.NewTraces(), err
		}
		return traces, nil
	}
	
	// If no OTLP bytes, return empty traces
	return ptrace.NewTraces(), nil
}

func (s *InMemoryStore) appendTraces(dest, src ptrace.Traces) {
	srcRS := src.ResourceSpans()
	for i := 0; i < srcRS.Len(); i++ {
		rs := dest.ResourceSpans().AppendEmpty()
		srcRS.At(i).CopyTo(rs)
	}
}

func (s *InMemoryStore) calculateApproxBytes(traces ptrace.Traces) int64 {
	// Simple approximation - could be more sophisticated
	return int64(traces.SpanCount() * 1024) // Assume ~1KB per span
}