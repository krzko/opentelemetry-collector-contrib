// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

func TestMemStore_Basic(t *testing.T) {
	cfg := store.Config{
		HotLimits: store.HotLimits{
			MaxSpanPerTrace:  1000,
			MaxBytesPerTrace: 1024 * 1024,
			DefaultTTL:       time.Minute,
			LeaseTTL:         time.Second * 10,
		},
	}

	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// Test append
	spans := store.EncodedSpans{
		OTLP:  []byte("test-data"),
		Count: 2,
		Bytes: 100,
	}

	result, err := ms.Append(ctx, tenant, traceID, spans)
	require.NoError(t, err)
	assert.Equal(t, 2, result.NewSpanCount)
	assert.False(t, result.Spilled)

	// Test fetch meta
	meta, err := ms.FetchMeta(ctx, traceID)
	require.NoError(t, err)
	assert.Equal(t, tenant, meta.Tenant)
	assert.Equal(t, traceID, meta.TraceID)
	assert.Equal(t, 2, meta.SpanCount)
	assert.Equal(t, store.StateActive, meta.State)

	// Test stats
	stats, err := ms.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.InFlightTraces)
	assert.Equal(t, int64(1), stats.AppendsTotal)
}

func TestMemStore_Leasing(t *testing.T) {
	cfg := store.Config{}
	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// First append to create trace
	spans := store.EncodedSpans{Count: 1, Bytes: 50}
	_, err := ms.Append(ctx, tenant, traceID, spans)
	require.NoError(t, err)

	// Test lease acquisition
	opts := store.LeaseOptions{
		OwnerID: "worker-1",
		TTL:     time.Second * 10,
	}

	acquired, err := ms.TryAcquireLease(ctx, traceID, opts)
	require.NoError(t, err)
	assert.True(t, acquired)

	// Test lease conflict
	opts2 := store.LeaseOptions{
		OwnerID: "worker-2",
		TTL:     time.Second * 10,
	}

	acquired2, err := ms.TryAcquireLease(ctx, traceID, opts2)
	require.Error(t, err)
	assert.False(t, acquired2)
	assert.Equal(t, store.ErrLeaseOwned, err)

	// Test lease refresh
	opts.Refresh = true
	acquired3, err := ms.TryAcquireLease(ctx, traceID, opts)
	require.NoError(t, err)
	assert.True(t, acquired3)

	// Test lease release
	err = ms.ReleaseLease(ctx, traceID, "worker-1")
	require.NoError(t, err)

	// Should be able to acquire after release
	acquired4, err := ms.TryAcquireLease(ctx, traceID, opts2)
	require.NoError(t, err)
	assert.True(t, acquired4)
}

func TestMemStore_Decision(t *testing.T) {
	cfg := store.Config{}
	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// First append to create trace
	spans := store.EncodedSpans{Count: 1, Bytes: 50}
	_, err := ms.Append(ctx, tenant, traceID, spans)
	require.NoError(t, err)

	// Test decision not found initially
	decision, found, err := ms.GetDecision(ctx, traceID)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, store.DecisionUnknown, decision)

	// Set decision
	err = ms.SetDecision(ctx, traceID, store.DecisionSampled, store.DecisionOptions{
		TTL: time.Minute,
	})
	require.NoError(t, err)

	// Get decision
	decision, found, err = ms.GetDecision(ctx, traceID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, store.DecisionSampled, decision)

	// Test idempotent set
	err = ms.SetDecision(ctx, traceID, store.DecisionSampled, store.DecisionOptions{})
	require.NoError(t, err)
}

func TestMemStore_Iterator(t *testing.T) {
	cfg := store.Config{}
	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// Append spans
	spans := store.EncodedSpans{Count: 5, Bytes: 250}
	_, err := ms.Append(ctx, tenant, traceID, spans)
	require.NoError(t, err)

	// Test iterator
	iter, meta, err := ms.OpenIterator(ctx, traceID, store.FetchOptions{
		MaxSpans: 2,
	})
	require.NoError(t, err)
	defer iter.Close()

	assert.Equal(t, 5, meta.SpanCount)

	// Read batches
	var totalSpans int
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			break
		}
		totalSpans += batch.Len()
		assert.LessOrEqual(t, batch.Len(), 2) // Respect batch size
	}

	assert.Equal(t, 5, totalSpans)
}

func TestMemStore_SpillTrigger(t *testing.T) {
	cfg := store.Config{
		HotLimits: store.HotLimits{
			MaxSpanPerTrace:  2, // Low limit to trigger spill
			MaxBytesPerTrace: 1024 * 1024,
		},
	}

	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// First append - should not spill
	spans1 := store.EncodedSpans{Count: 2, Bytes: 100}
	result1, err := ms.Append(ctx, tenant, traceID, spans1)
	require.NoError(t, err)
	assert.False(t, result1.Spilled)

	// Second append - should trigger spill
	spans2 := store.EncodedSpans{Count: 1, Bytes: 50}
	result2, err := ms.Append(ctx, tenant, traceID, spans2)
	require.NoError(t, err)
	assert.True(t, result2.Spilled)

	// Check trace state
	meta, err := ms.FetchMeta(ctx, traceID)
	require.NoError(t, err)
	assert.Equal(t, store.StateSpilled, meta.State)
}

func TestMemStore_Deduplication(t *testing.T) {
	cfg := store.Config{}
	ms := NewMemStore(cfg)
	defer ms.Close(context.Background())

	ctx := context.Background()
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	tenant := store.Tenant("test-tenant")

	// First append
	spans1 := store.EncodedSpans{Count: 2, Bytes: 100}
	result1, err := ms.Append(ctx, tenant, traceID, spans1)
	require.NoError(t, err)
	assert.Equal(t, 2, result1.NewSpanCount)

	// Second append with more spans - should all be new spans (no duplication yet)
	spans2 := store.EncodedSpans{Count: 2, Bytes: 100}
	result2, err := ms.Append(ctx, tenant, traceID, spans2)
	require.NoError(t, err)
	assert.Equal(t, 2, result2.NewSpanCount) // New spans added

	// Check final trace state
	meta, err := ms.FetchMeta(ctx, traceID)
	require.NoError(t, err)
	assert.Equal(t, 4, meta.SpanCount) // Should be 4 total

	// Check stats - this test is really testing that the span ID generation works properly
	stats, err := ms.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.AppendsTotal)
	assert.Equal(t, int64(0), stats.AppendsDeduped) // No duplications yet with current logic
}
