// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"errors"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// Circuit breaker errors
var (
	ErrCircuitOpen = errors.New("redis: circuit breaker is open")
)

// TraceKeys holds all Redis keys for a specific trace
type TraceKeys struct {
	Meta      string // ts:trace:{id}:meta (HASH)
	Spans     string // ts:trace:{id}:spans (LIST)
	Dedup     string // ts:trace:{id}:dedup (HASH)
	Segments  string // ts:trace:{id}:segments (LIST)
	Decision  string // ts:trace:{id}:decision (STRING)
	Lease     string // ts:trace:{id}:lease (STRING with TTL)
	Active    string // ts:active (ZSET - global active traces)
}

// StoreStats tracks Redis store operational metrics
type StoreStats struct {
	// Operation counts
	AppendCalls        int64
	AppendErrors       int64
	FetchAllCalls      int64
	FetchAllErrors     int64
	MetaFetchCalls     int64
	MetaFetchErrors    int64
	IteratorCalls      int64
	IteratorErrors     int64
	LeaseCalls         int64
	LeaseErrors        int64
	LeaseReleaseCalls  int64
	LeaseReleaseErrors int64
	DecisionSetCalls   int64
	DecisionSetErrors  int64
	DecisionGetCalls   int64
	DecisionGetErrors  int64
	CompleteCalls      int64
	CompleteErrors     int64
	SpillCalls         int64
	SpillErrors        int64
	RehydrateCalls     int64
	RehydrateErrors    int64

	// Resource metrics
	TracesActive     int64
	SpansBuffered    int64
	BytesBuffered    int64
	LeasesAcquired   int64
	LeasesReleased   int64
	LeaseConflicts   int64
}

// Request structures for Lua scripts

// AppendRequest holds parameters for the append_and_index.lua script
type AppendRequest struct {
	Keys             TraceKeys
	Tenant           string
	TraceID          store.TraceID
	SpanID           string
	SpanPayload      []byte
	SpanPayloadLen   int
	TTL              time.Duration
	DecisionWait     time.Duration
	MaxSpansPerTrace int
	MaxBytesPerTrace int64
	LeaseOwner       string
	RefreshLease     bool
}

// AppendResponse holds the result from append_and_index.lua script
type AppendResponse struct {
	Appended       int  // 1 if span was appended (not duplicate), 0 otherwise
	Created        int  // 1 if trace was created for first time, 0 otherwise
	OverLimit      int  // 1 if trace exceeded limits and needs spilling, 0 otherwise
	SpanCount      int  // current span count after append
	ApproxBytes    int64 // current approx bytes after append
	AlreadyDecided bool  // true if decision already exists (early exit)
}

// LeaseRequest holds parameters for the lease_acquire.lua script
type LeaseRequest struct {
	LeaseKey string
	OwnerID  string
	TTL      time.Duration
	Refresh  bool
}

// LeaseReleaseRequest holds parameters for the lease_release.lua script
type LeaseReleaseRequest struct {
	LeaseKey string
	OwnerID  string
}

// DecisionRequest holds parameters for the decision_set.lua script
type DecisionRequest struct {
	DecisionKey string
	Decision    store.Decision
	TTL         time.Duration
	Overwrite   bool
	Reason      string
}

// CompleteRequest holds parameters for the complete_and_gc.lua script
type CompleteRequest struct {
	Keys    TraceKeys
	TraceID store.TraceID
	Hard    bool
	MaxKeys int
}

// SpanPayload represents a serialized span for Redis storage
type SpanPayload struct {
	Data   []byte
	Length int
}

// RedisSpanIterator is implemented in redis_iterator.go