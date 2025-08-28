// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemory

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/adapter"
)

func TestInMemoryStore_Compatibility(t *testing.T) {
	// Create an in-memory store
	var droppedTraces []pcommon.TraceID
	dropFn := func(traceID pcommon.TraceID, time time.Time) {
		droppedTraces = append(droppedTraces, traceID)
	}
	
	inmemoryStore := NewInMemoryStore(dropFn)
	ctx := context.Background()
	
	// Create test trace data
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetTraceID(traceID)
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("test-span")
	
	// Convert to EncodedSpans using adapter
	adp := adapter.NewPtraceAdapter()
	encoded, err := adp.EncodePtraceToSpans(traces)
	require.NoError(t, err)
	
	// Test Append
	result, err := inmemoryStore.Append(ctx, "test-tenant", traceID, encoded)
	require.NoError(t, err)
	assert.Equal(t, 1, result.NewSpanCount)
	assert.False(t, result.Spilled)
	
	// Test TryAcquireLease (should always succeed for legacy)
	acquired, err := inmemoryStore.TryAcquireLease(ctx, traceID, store.LeaseOptions{
		OwnerID: "test-owner",
		TTL:     time.Minute,
	})
	require.NoError(t, err)
	assert.True(t, acquired)
	
	// Test FetchMeta
	meta, err := inmemoryStore.FetchMeta(ctx, traceID)
	require.NoError(t, err)
	assert.Equal(t, traceID, meta.TraceID)
	assert.Equal(t, 1, meta.SpanCount)
	assert.Equal(t, store.StateActive, meta.State)
	assert.False(t, meta.HasSpill)
	
	// Test FetchAll
	decodedSpans, fetchedMeta, err := inmemoryStore.FetchAll(ctx, traceID, store.FetchOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, decodedSpans.Len())
	assert.Equal(t, traceID, fetchedMeta.TraceID)
	
	// Verify span data
	spanRecord := decodedSpans.At(0)
	assert.Equal(t, traceID, spanRecord.TraceID)
	assert.Equal(t, "test-span", spanRecord.Name)
	
	// Test OpenIterator
	iterator, iterMeta, err := inmemoryStore.OpenIterator(ctx, traceID, store.FetchOptions{})
	require.NoError(t, err)
	assert.Equal(t, traceID, iterMeta.TraceID)
	
	// Read from iterator
	batch, err := iterator.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, batch.Len())
	
	// Should be EOF on next call
	_, err = iterator.Next(ctx)
	assert.Error(t, err)
	
	iterator.Close()
	
	// Test SetDecision
	err = inmemoryStore.SetDecision(ctx, traceID, store.DecisionSampled, store.DecisionOptions{})
	require.NoError(t, err)
	
	// Test GetDecision
	decision, found, err := inmemoryStore.GetDecision(ctx, traceID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, store.DecisionSampled, decision)
	
	// Test MarkComplete
	err = inmemoryStore.MarkComplete(ctx, traceID, store.GCOptions{})
	require.NoError(t, err)
	
	// Verify trace was dropped
	assert.Len(t, droppedTraces, 1)
	assert.Equal(t, traceID, droppedTraces[0])
	
	// Test Stats
	stats, err := inmemoryStore.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.InFlightTraces) // Should be 0 after MarkComplete
}

func TestInMemoryStore_DirectAccess(t *testing.T) {
	// Test the direct access methods for processor compatibility
	inmemoryStore := NewInMemoryStore(nil)
	
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	
	// Test GetTraceData on non-existent trace
	data, found := inmemoryStore.GetTraceData(traceID)
	assert.False(t, found)
	assert.Nil(t, data)
	
	// Test LoadOrStoreTraceData
	testData := &sampling.TraceData{
		ArrivalTime: time.Now(),
		SpanCount:   &atomic.Int64{},
	}
	testData.SpanCount.Store(1)
	
	stored, loaded := inmemoryStore.LoadOrStoreTraceData(traceID, testData)
	assert.False(t, loaded)
	assert.Equal(t, testData, stored)
	
	// Test GetTraceData on existing trace
	data, found = inmemoryStore.GetTraceData(traceID)
	assert.True(t, found)
	assert.Equal(t, testData, data)
	
	// Test LoadOrStoreTraceData on existing trace
	newData := &sampling.TraceData{
		ArrivalTime: time.Now().Add(time.Hour),
		SpanCount:   &atomic.Int64{},
	}
	stored, loaded = inmemoryStore.LoadOrStoreTraceData(traceID, newData)
	assert.True(t, loaded)
	assert.Equal(t, testData, stored) // Should return original data
	assert.NotEqual(t, newData, stored)
}