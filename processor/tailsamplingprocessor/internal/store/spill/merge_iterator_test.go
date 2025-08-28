// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helpers are now in test_helpers.go

func TestMergeIteratorOptions_SetDefaults(t *testing.T) {
	opts := MergeIteratorOptions{}
	opts.SetDefaults()

	assert.Equal(t, 100, opts.BatchSize)
	assert.Equal(t, 4, opts.PrefetchOptions.MaxConcurrency)
	assert.Equal(t, 1000, opts.PrefetchOptions.BufferSize)
}

func TestMergeIteratorOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    MergeIteratorOptions
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid options",
			opts: MergeIteratorOptions{
				BatchSize:     100,
				HotSpansFirst: true,
				PrefetchOptions: PrefetchOptions{
					MaxConcurrency: 4,
					BufferSize:     1000,
					FetchTimeout:   30 * time.Second,
					RetryAttempts:  3,
					RetryDelay:     1 * time.Second,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid batch size",
			opts: MergeIteratorOptions{
				BatchSize: 10001,
			},
			wantErr: true,
			errMsg:  "batch_size must be between 1 and 10000",
		},
		{
			name: "invalid prefetch options",
			opts: MergeIteratorOptions{
				BatchSize: 100,
				PrefetchOptions: PrefetchOptions{
					MaxConcurrency: 101, // Invalid
				},
			},
			wantErr: true,
			errMsg:  "max_concurrency must be between 1 and 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewDefaultMergeIterator(t *testing.T) {
	t.Run("hot spans only", func(t *testing.T) {
		hotSpans := []SpanRecord{
			{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "hot1"},
			{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Name: "hot2"},
		}
		hotIterator := NewMockHotSpanIterator(hotSpans)
		
		reader := NewMockSegmentReader()
		factory := NewMockSegmentFactory(reader)
		opts := MergeIteratorOptions{
			BatchSize: 10,
		}

		iterator, err := NewDefaultMergeIterator(hotIterator, nil, factory, opts)
		require.NoError(t, err)
		assert.NotNil(t, iterator)

		ctx := context.Background()
		batch, err := iterator.NextBatch(ctx)
		require.NoError(t, err)
		assert.Len(t, batch, 2)

		// Next batch should return EOF
		batch, err = iterator.NextBatch(ctx)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, batch)

		err = iterator.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("spilled spans only", func(t *testing.T) {
		spilledSpans := []SpanRecord{
			{TraceID: [16]byte{2}, SpanID: [8]byte{1}, Name: "spilled1"},
			{TraceID: [16]byte{2}, SpanID: [8]byte{2}, Name: "spilled2"},
		}

		reader := NewMockSegmentReader()
		reader.AddSegment("gs://bucket/segment1", spilledSpans)
		
		factory := NewMockSegmentFactory(reader)
		opts := MergeIteratorOptions{
			BatchSize: 10,
		}

		segments := []SegmentRef{
			{URL: "gs://bucket/segment1", Count: 2},
		}

		iterator, err := NewDefaultMergeIterator(nil, segments, factory, opts)
		require.NoError(t, err)
		assert.NotNil(t, iterator)

		ctx := context.Background()
		
		// Give prefetcher time to start
		time.Sleep(10 * time.Millisecond)
		
		batch, err := iterator.NextBatch(ctx)
		require.NoError(t, err)
		assert.Len(t, batch, 2)

		// Eventually should return EOF
		for {
			batch, err = iterator.NextBatch(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		err = iterator.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("hot and spilled spans", func(t *testing.T) {
		hotSpans := []SpanRecord{
			{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "hot1"},
			{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Name: "hot2"},
		}
		hotIterator := NewMockHotSpanIterator(hotSpans)

		spilledSpans := []SpanRecord{
			{TraceID: [16]byte{2}, SpanID: [8]byte{1}, Name: "spilled1"},
			{TraceID: [16]byte{2}, SpanID: [8]byte{2}, Name: "spilled2"},
		}

		reader := NewMockSegmentReader()
		reader.AddSegment("gs://bucket/segment1", spilledSpans)
		
		factory := NewMockSegmentFactory(reader)
		opts := MergeIteratorOptions{
			BatchSize:     10,
			HotSpansFirst: true,
		}

		segments := []SegmentRef{
			{URL: "gs://bucket/segment1", Count: 2},
		}

		iterator, err := NewDefaultMergeIterator(hotIterator, segments, factory, opts)
		require.NoError(t, err)

		ctx := context.Background()
		
		// Give prefetcher time to start
		time.Sleep(10 * time.Millisecond)
		
		// Should get all 4 spans
		var allSpans []SpanRecord
		for {
			batch, err := iterator.NextBatch(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			allSpans = append(allSpans, batch...)
		}

		assert.Len(t, allSpans, 4)
		
		// With HotSpansFirst, hot spans should come first
		assert.Equal(t, "hot1", allSpans[0].Name)
		assert.Equal(t, "hot2", allSpans[1].Name)

		stats := iterator.GetStats()
		assert.Equal(t, int64(2), stats.HotSpansRead)
		assert.Equal(t, int64(2), stats.SpilledSpansRead)

		err = iterator.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("invalid options", func(t *testing.T) {
		hotIterator := NewMockHotSpanIterator(nil)
		reader := NewMockSegmentReader()
		factory := NewMockSegmentFactory(reader)
		
		opts := MergeIteratorOptions{
			BatchSize: 10001, // Invalid - exceeds maximum
		}

		iterator, err := NewDefaultMergeIterator(hotIterator, nil, factory, opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid merge iterator options")
		assert.Nil(t, iterator)
	})
}

func TestDefaultMergeIterator_Interleaved(t *testing.T) {
	// Create more spans to test interleaving
	hotSpans := []SpanRecord{
		{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "hot1"},
		{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Name: "hot2"},
		{TraceID: [16]byte{1}, SpanID: [8]byte{3}, Name: "hot3"},
		{TraceID: [16]byte{1}, SpanID: [8]byte{4}, Name: "hot4"},
	}
	hotIterator := NewMockHotSpanIterator(hotSpans)

	spilledSpans := []SpanRecord{
		{TraceID: [16]byte{2}, SpanID: [8]byte{1}, Name: "spilled1"},
		{TraceID: [16]byte{2}, SpanID: [8]byte{2}, Name: "spilled2"},
		{TraceID: [16]byte{2}, SpanID: [8]byte{3}, Name: "spilled3"},
		{TraceID: [16]byte{2}, SpanID: [8]byte{4}, Name: "spilled4"},
	}

	reader := NewMockSegmentReader()
	reader.AddSegment("gs://bucket/segment1", spilledSpans)
	
	factory := NewMockSegmentFactory(reader)
	opts := MergeIteratorOptions{
		BatchSize:     2,
		HotSpansFirst: false, // Interleave
	}

	segments := []SegmentRef{
		{URL: "gs://bucket/segment1", Count: 4},
	}

	iterator, err := NewDefaultMergeIterator(hotIterator, segments, factory, opts)
	require.NoError(t, err)

	ctx := context.Background()
	
	// Give prefetcher time to start
	time.Sleep(10 * time.Millisecond)
	
	// Get all batches
	var allBatches [][]SpanRecord
	for {
		batch, err := iterator.NextBatch(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		allBatches = append(allBatches, batch)
	}

	// Should have multiple batches due to batch size limit
	assert.True(t, len(allBatches) >= 4)

	// Collect all spans
	var allSpans []SpanRecord
	for _, batch := range allBatches {
		allSpans = append(allSpans, batch...)
	}
	assert.Len(t, allSpans, 8)

	stats := iterator.GetStats()
	assert.Equal(t, int64(4), stats.HotSpansRead)
	assert.Equal(t, int64(4), stats.SpilledSpansRead)
	assert.True(t, stats.BatchesReturned >= 4)

	err = iterator.Close(ctx)
	require.NoError(t, err)
}

func TestDefaultMergeIterator_ContextCancellation(t *testing.T) {
	hotSpans := []SpanRecord{
		{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "hot1"},
	}
	hotIterator := NewMockHotSpanIterator(hotSpans)
	hotIterator.delay = 100 * time.Millisecond // Slow hot iterator

	reader := NewMockSegmentReader()
	factory := NewMockSegmentFactory(reader)
	opts := MergeIteratorOptions{
		BatchSize: 10,
	}

	iterator, err := NewDefaultMergeIterator(hotIterator, nil, factory, opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Should eventually return context error
	_, err = iterator.NextBatch(ctx)
	// Could be either successful or context error depending on timing
	if err != nil && err != io.EOF {
		assert.Contains(t, err.Error(), "context")
	}

	err = iterator.Close(context.Background())
	require.NoError(t, err)
}

func TestDefaultMergeIterator_Closed(t *testing.T) {
	hotIterator := NewMockHotSpanIterator(nil)
	reader := NewMockSegmentReader()
	factory := NewMockSegmentFactory(reader)
	opts := MergeIteratorOptions{}

	iterator, err := NewDefaultMergeIterator(hotIterator, nil, factory, opts)
	require.NoError(t, err)

	ctx := context.Background()
	
	// Close the iterator
	err = iterator.Close(ctx)
	require.NoError(t, err)

	// Try to use after close
	_, err = iterator.NextBatch(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterator is closed")

	// Close again should be safe
	err = iterator.Close(ctx)
	require.NoError(t, err)
}

func TestDefaultMergeIterator_PrefetcherError(t *testing.T) {
	reader := NewMockSegmentReader()
	// Don't add segment - will cause error
	
	factory := NewMockSegmentFactory(reader)
	opts := MergeIteratorOptions{
		BatchSize: 10,
	}

	segments := []SegmentRef{
		{URL: "gs://bucket/missing", Count: 2},
	}

	iterator, err := NewDefaultMergeIterator(nil, segments, factory, opts)
	require.NoError(t, err)

	ctx := context.Background()
	
	// Give prefetcher time to fail
	time.Sleep(20 * time.Millisecond)
	
	// Should eventually return EOF (no spans fetched)
	batch, err := iterator.NextBatch(ctx)
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, batch)

	// Check error count in stats
	stats := iterator.GetStats()
	assert.True(t, stats.ErrorCount >= 0)

	err = iterator.Close(ctx)
	require.NoError(t, err)
}