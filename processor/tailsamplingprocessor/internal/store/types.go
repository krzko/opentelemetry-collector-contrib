// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TraceID uses pcommon.TraceID directly for compatibility with existing processor.
type TraceID = pcommon.TraceID

// SpanKey identifies a span within a trace.
type SpanKey struct {
	TraceID TraceID
	SpanID  pcommon.SpanID
}

// Tenant is a logical partition key (org, project, env).
type Tenant string

// Decision is the final sampling outcome.
type Decision int8

const (
	DecisionUnknown Decision = iota
	DecisionSampled
	DecisionNotSampled
)

// State captures the lifecycle of a trace record.
type State int8

const (
	StateActive    State = iota // receiving spans
	StateSpilled                // active but some segments spilled
	StateDecided                // decision persisted
	StateCompleted              // GC eligible
)

// Meta summarises per-trace attributes required for policy timing and guardrails.
type Meta struct {
	Tenant      Tenant
	TraceID     TraceID
	FirstSeen   time.Time
	LastSeen    time.Time
	SpanCount   int
	ApproxBytes int64
	State       State
	HasSpill    bool
	Segments    []SpillSegmentRef // zero if no spill
	SchemaVer   int               // spill schema versioning
	Owner       string            // best-effort: current lease holder
	LeaseExpiry time.Time         // best-effort: lease expiry
	Custom      map[string]string // optional tags (e.g., priority)
}

// SpillSegmentRef locates a segment in object storage.
type SpillSegmentRef struct {
	URL        string     // gs://... or s3://...
	Codec      SpillCodec // Avro or JSONL_ZSTD
	FirstIndex int        // first appended span index in this segment
	Count      int        // number of spans in segment
	Bytes      int64
	CreatedAt  time.Time
}

// SpillCodec enumerates on-disk formats.
type SpillCodec int8

const (
	SpillCodecAvro SpillCodec = iota
	SpillCodecJSONLZstd
)

// AppendResult reports what happened after an append.
type AppendResult struct {
	NewSpanCount   int
	NewApproxBytes int64
	Spilled        bool
}

// LeaseOptions control per-trace ownership.
type LeaseOptions struct {
	OwnerID string        // unique per process (e.g., hostname/pod#pid)
	TTL     time.Duration // recommended 5–15s
	Refresh bool          // if true, renews existing lease if owner matches
}

// FetchOptions control evaluation reads.
type FetchOptions struct {
	// If true, returns an iterator instead of materialising all spans into RAM.
	Streaming bool

	// Optional: maximum spans/bytes to read in one shot when materialising.
	MaxSpans int
	MaxBytes int64

	// If true, the store may prefetch spill segments from object storage.
	PrefetchSpill bool
}

// DecisionOptions control how decisions are stored.
type DecisionOptions struct {
	TTL            time.Duration // keep decision cached to suppress rework
	Overwrite      bool          // allow overwrite if already present (normally false)
	IncludeSummary bool          // store a compact reason/summary if backend supports it
	Reason         string
}

// GCOptions control garbage collection.
type GCOptions struct {
	Hard bool // if true, delete keys immediately; else rely on TTLs
	// Optional cap to bound the GC work per call.
	MaxKeys int
}

// EncodedSpans represents a batch as provided by the processor to the store.
// Implementations can support one or more encodings; at minimum OTLP Protobuf
// wire bytes and/or a compact columnar-in-rows (CIR) internal format.
type EncodedSpans struct {
	// Exactly one should be non-nil. Implementations MAY support both.
	OTLP   []byte // marshalled ptrace.Traces or ptrace.ResourceSpans for a single trace
	Native []byte // impl-defined packed format (e.g., msgpack/json/flatbuffers)
	Count  int    // number of spans encoded
	Bytes  int    // payload size; hint for spill thresholds
}

// DecodedSpans provides a zero-copy or streaming view for evaluation.
// To avoid pulling in OTel deps here, we model a minimal accessor API.
type DecodedSpans interface {
	// Len is total spans in view.
	Len() int
	// At returns a lightweight decoded record; implementations may reuse memory.
	At(i int) SpanRecord
}

// SpanRecord is the minimal subset needed for policies (latency, status, attributes, name).
// Implementations can carry an opaque `Raw` field allowing evaluator to reconstruct full pdata span if needed.
type SpanRecord struct {
	TraceID       TraceID
	SpanID        pcommon.SpanID
	ParentSpanID  pcommon.SpanID
	Name          string
	Kind          int8
	StartUnixNano uint64
	EndUnixNano   uint64
	StatusCode    int8           // OTel StatusCode
	Attributes    map[string]any // string, bool, int64, float64; short-lived
	Raw           []byte         // optional: underlying OTLP bytes for this span
}

// SpanIterator streams SpanRecord batches (implementation-specific chunking).
type SpanIterator interface {
	// Next returns the next batch. Returns (nil, io.EOF) at end.
	Next(ctx context.Context) (DecodedSpans, error)
	// Close releases resources.
	Close() error
}

// Stats exposes store-wide counters and gauges for metrics.
type Stats struct {
	InFlightTraces      int64
	HotBytes            int64
	SpillSegments       int64
	SpillBytes          int64
	AppendsTotal        int64
	AppendsDeduped      int64
	LeasesGranted       int64
	LeaseConflicts      int64
	DecisionsSampled    int64
	DecisionsNotSampled int64
	DecisionsCachedHits int64
	LateSpans           int64
	Errors              map[string]int64 // keyed by error string/class
}
