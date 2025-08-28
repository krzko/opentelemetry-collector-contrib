// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
)

// TraceBufferStore encapsulates per-trace buffering and decision state.
//
// Concurrency:
//   - All methods are safe for concurrent use.
//   - The store enforces single-writer semantics *per trace* via leases.
//   - Append is idempotent per (trace_id, span_id).
//
// Time & TTLs:
//   - The store uses monotonic time (time.Now()) only for relative durations.
//   - Processor supplies wall clock via Meta.FirstSeen/LastSeen; store may also stamp.
//
// Errors:
//   - Return typed errors (see errors.go) for control-flow decisions in the processor.
type TraceBufferStore interface {
	// Append a batch of spans to the trace buffer.
	// Spans MUST be for a single trace/tenant. The store MUST deduplicate by span_id.
	//
	// Contracts:
	//   - Idempotent per (TraceID, SpanID)
	//   - Updates Meta.LastSeen, SpanCount, ApproxBytes
	//   - May spill automatically if thresholds exceeded
	//
	Append(ctx context.Context, tenant Tenant, traceID TraceID, spans EncodedSpans) (AppendResult, error)

	// TryAcquireLease obtains an exclusive short-term lease on a trace for evaluation/compaction.
	// If Refresh==true and caller holds the lease, it MUST extend the lease TTL.
	//
	// Returns true if the caller owns the lease after the call.
	//
	TryAcquireLease(ctx context.Context, traceID TraceID, opts LeaseOptions) (bool, error)

	// ReleaseLease voluntarily releases ownership (best-effort).
	ReleaseLease(ctx context.Context, traceID TraceID, ownerID string) error

	// FetchMeta returns current metadata without loading spans.
	FetchMeta(ctx context.Context, traceID TraceID) (Meta, error)

	// FetchAll returns all spans for evaluation (materialised), plus Meta snapshot.
	// If FetchOptions.Streaming==true, returns ErrUseIterator.
	//
	FetchAll(ctx context.Context, traceID TraceID, opts FetchOptions) (DecodedSpans, Meta, error)

	// Iterator API for streaming evaluation to minimise RAM usage.
	// OpenIterator creates a cursor across in-memory chunks and spill segments.
	OpenIterator(ctx context.Context, traceID TraceID, opts FetchOptions) (SpanIterator, Meta, error)

	// SetDecision persists the final sampling decision.
	// Idempotent: re-setting the same decision is a no-op.
	SetDecision(ctx context.Context, traceID TraceID, decision Decision, opts DecisionOptions) error

	// GetDecision returns the cached decision if present.
	GetDecision(ctx context.Context, traceID TraceID) (Decision, bool, error)

	// MarkComplete marks the trace as GC-eligible (post-export/drop).
	// The store may delete spans immediately (Hard GC) or let TTLs expire.
	MarkComplete(ctx context.Context, traceID TraceID, gc GCOptions) error

	// SpillNow forces a spill according to backend thresholds/policy.
	// Typically used when Redis is near capacity or backend is down.
	SpillNow(ctx context.Context, traceID TraceID) (SpillSegmentRef, error)

	// Rehydrate loads spilled data (fully or partially) back to hot storage.
	// The store implementation decides whether to materialise or re-index only.
	Rehydrate(ctx context.Context, traceID TraceID) error

	// Stats exposes store-wide counters and gauges for metrics.
	Stats(ctx context.Context) (Stats, error)

	// Close releases any background resources (connections, pools).
	Close(ctx context.Context) error
}
