// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helpers are now in test_helpers.go

func TestPrefetchOptions_SetDefaults(t *testing.T) {
	opts := PrefetchOptions{}
	opts.SetDefaults()

	assert.Equal(t, 4, opts.MaxConcurrency)
	assert.Equal(t, 1000, opts.BufferSize)
	assert.Equal(t, 30*time.Second, opts.FetchTimeout)
	assert.Equal(t, 3, opts.RetryAttempts)
	assert.Equal(t, 1*time.Second, opts.RetryDelay)
}

func TestPrefetchOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    PrefetchOptions
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid options",
			opts: PrefetchOptions{
				MaxConcurrency: 4,
				BufferSize:     1000,
				FetchTimeout:   30 * time.Second,
				RetryAttempts:  3,
				RetryDelay:     1 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "invalid max concurrency",
			opts: PrefetchOptions{
				MaxConcurrency: 101,
				BufferSize:     1000,
				FetchTimeout:   30 * time.Second,
				RetryAttempts:  3,
				RetryDelay:     1 * time.Second,
			},
			wantErr: true,
			errMsg:  "max_concurrency must be between 1 and 100",
		},
		{
			name: "invalid buffer size",
			opts: PrefetchOptions{
				MaxConcurrency: 4,
				BufferSize:     100001,
				FetchTimeout:   30 * time.Second,
				RetryAttempts:  3,
				RetryDelay:     1 * time.Second,
			},
			wantErr: true,
			errMsg:  "buffer_size must be between 1 and 100000",
		},
		{
			name: "invalid fetch timeout",
			opts: PrefetchOptions{
				MaxConcurrency: 4,
				BufferSize:     1000,
				FetchTimeout:   6 * time.Minute,
				RetryAttempts:  3,
				RetryDelay:     1 * time.Second,
			},
			wantErr: true,
			errMsg:  "fetch_timeout must be between 1s and 5m",
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

func TestNewDefaultPrefetcher(t *testing.T) {
	t.Run("valid factory and options", func(t *testing.T) {
		factory := NewMockSegmentFactory(NewMockSegmentReader())
		opts := PrefetchOptions{
			MaxConcurrency: 2,
			BufferSize:     100,
		}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)
		assert.NotNil(t, prefetcher)
	})

	t.Run("nil factory", func(t *testing.T) {
		opts := PrefetchOptions{}
		
		prefetcher, err := NewDefaultPrefetcher(nil, opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "segment factory is required")
		assert.Nil(t, prefetcher)
	})

	t.Run("invalid options", func(t *testing.T) {
		factory := NewMockSegmentFactory(NewMockSegmentReader())
		opts := PrefetchOptions{
			MaxConcurrency: 101, // Invalid - exceeds maximum
		}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid prefetch options")
		assert.Nil(t, prefetcher)
	})
}

func TestDefaultPrefetcher_Start(t *testing.T) {
	t.Run("successful fetch", func(t *testing.T) {
		// Create test data
		spans1 := []SpanRecord{
			{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "span1"},
			{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Name: "span2"},
		}
		spans2 := []SpanRecord{
			{TraceID: [16]byte{2}, SpanID: [8]byte{1}, Name: "span3"},
			{TraceID: [16]byte{2}, SpanID: [8]byte{2}, Name: "span4"},
		}

		reader := NewMockSegmentReader()
		reader.AddSegment("gs://bucket/segment1", spans1)
		reader.AddSegment("gs://bucket/segment2", spans2)

		factory := NewMockSegmentFactory(reader)
		opts := PrefetchOptions{
			MaxConcurrency: 2,
			BufferSize:     10,
		}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)

		segments := []SegmentRef{
			{URL: "gs://bucket/segment1", Count: 2},
			{URL: "gs://bucket/segment2", Count: 2},
		}

		ctx := context.Background()
		spanChan, err := prefetcher.Start(ctx, segments)
		require.NoError(t, err)
		require.NotNil(t, spanChan)

		// Collect all spans
		var receivedSpans []SpanRecord
		for span := range spanChan {
			receivedSpans = append(receivedSpans, span)
		}

		assert.Len(t, receivedSpans, 4)
		
		// Check stats
		stats := prefetcher.GetStats()
		assert.Equal(t, 2, stats.SegmentsTotal)
		assert.Equal(t, 2, stats.SegmentsCompleted)
		assert.Equal(t, 0, stats.SegmentsFailed)
		assert.Equal(t, int64(4), stats.SpansFetched)
		
		// Cleanup
		err = prefetcher.Close()
		require.NoError(t, err)
	})

	t.Run("empty segments", func(t *testing.T) {
		reader := NewMockSegmentReader()
		factory := NewMockSegmentFactory(reader)
		opts := PrefetchOptions{}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)

		ctx := context.Background()
		spanChan, err := prefetcher.Start(ctx, []SegmentRef{})
		require.NoError(t, err)
		require.NotNil(t, spanChan)

		// Channel should be closed immediately
		_, ok := <-spanChan
		assert.False(t, ok)
		
		err = prefetcher.Close()
		require.NoError(t, err)
	})

	t.Run("fetch with retries", func(t *testing.T) {
		spans := []SpanRecord{
			{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "span1"},
		}

		reader := NewMockSegmentReader()
		reader.AddSegment("gs://bucket/segment1", spans)
		reader.failAfter = 0 // Fail first attempt

		factory := NewMockSegmentFactory(reader)
		// First reader creation will return the failing reader
		// Subsequent creations need new readers
		
		opts := PrefetchOptions{
			MaxConcurrency: 1,
			BufferSize:     10,
			RetryAttempts:  2,
			RetryDelay:     1 * time.Millisecond,
		}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)

		segments := []SegmentRef{
			{URL: "gs://bucket/segment1", Count: 1},
		}

		ctx := context.Background()
		spanChan, err := prefetcher.Start(ctx, segments)
		require.NoError(t, err)

		// Collect errors
		go func() {
			for err := range prefetcher.GetErrors() {
				assert.NotNil(t, err)
			}
		}()

		// Should eventually fail after retries
		var receivedSpans []SpanRecord
		for span := range spanChan {
			receivedSpans = append(receivedSpans, span)
		}

		// Check stats
		stats := prefetcher.GetStats()
		assert.Equal(t, 1, stats.SegmentsFailed)
		
		err = prefetcher.Close()
		require.NoError(t, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		spans := []SpanRecord{
			{TraceID: [16]byte{1}, SpanID: [8]byte{1}, Name: "span1"},
		}

		reader := NewMockSegmentReader()
		reader.AddSegment("gs://bucket/segment1", spans)
		reader.readDelay = 100 * time.Millisecond // Slow reader

		factory := NewMockSegmentFactory(reader)
		opts := PrefetchOptions{
			MaxConcurrency: 1,
			BufferSize:     10,
		}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)

		segments := []SegmentRef{
			{URL: "gs://bucket/segment1", Count: 1},
		}

		ctx, cancel := context.WithCancel(context.Background())
		spanChan, err := prefetcher.Start(ctx, segments)
		require.NoError(t, err)

		// Cancel context quickly
		cancel()

		// Collect spans (should be incomplete)
		var receivedSpans []SpanRecord
		for span := range spanChan {
			receivedSpans = append(receivedSpans, span)
		}

		// Should have incomplete results
		assert.True(t, len(receivedSpans) <= 1)
		
		err = prefetcher.Close()
		require.NoError(t, err)
	})

	t.Run("closed prefetcher", func(t *testing.T) {
		reader := NewMockSegmentReader()
		factory := NewMockSegmentFactory(reader)
		opts := PrefetchOptions{}
		
		prefetcher, err := NewDefaultPrefetcher(factory, opts)
		require.NoError(t, err)

		// Close the prefetcher
		err = prefetcher.Close()
		require.NoError(t, err)

		// Try to start after close
		ctx := context.Background()
		_, err = prefetcher.Start(ctx, []SegmentRef{{URL: "test"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "prefetcher is closed")
	})
}

func TestDefaultPrefetcher_Concurrent(t *testing.T) {
	// Create many segments
	reader := NewMockSegmentReader()
	var segments []SegmentRef
	
	for i := 0; i < 10; i++ {
		url := fmt.Sprintf("gs://bucket/segment%d", i)
		spans := []SpanRecord{
			{TraceID: [16]byte{byte(i)}, SpanID: [8]byte{1}, Name: fmt.Sprintf("span%d-1", i)},
			{TraceID: [16]byte{byte(i)}, SpanID: [8]byte{2}, Name: fmt.Sprintf("span%d-2", i)},
		}
		reader.AddSegment(url, spans)
		segments = append(segments, SegmentRef{URL: url, Count: 2})
	}

	factory := NewMockSegmentFactory(reader)
	opts := PrefetchOptions{
		MaxConcurrency: 4, // Use 4 workers
		BufferSize:     100,
	}
	
	prefetcher, err := NewDefaultPrefetcher(factory, opts)
	require.NoError(t, err)

	ctx := context.Background()
	spanChan, err := prefetcher.Start(ctx, segments)
	require.NoError(t, err)

	// Collect all spans
	var receivedSpans []SpanRecord
	for span := range spanChan {
		receivedSpans = append(receivedSpans, span)
	}

	assert.Len(t, receivedSpans, 20)
	
	// Check stats
	stats := prefetcher.GetStats()
	assert.Equal(t, 10, stats.SegmentsTotal)
	assert.Equal(t, 10, stats.SegmentsCompleted)
	assert.Equal(t, 0, stats.SegmentsFailed)
	assert.Equal(t, int64(20), stats.SpansFetched)
	
	// Check that we used concurrency (created multiple readers)
	assert.True(t, factory.GetReaderCount() >= 4)
	
	err = prefetcher.Close()
	require.NoError(t, err)
}

func TestParseBackendFromURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    Backend
		wantErr bool
	}{
		{
			name:    "GCS URL",
			url:     "gs://bucket/path/to/object",
			want:    GCS,
			wantErr: false,
		},
		{
			name:    "S3 URL",
			url:     "s3://bucket/path/to/object",
			want:    S3,
			wantErr: false,
		},
		{
			name:    "invalid URL",
			url:     "http://example.com",
			wantErr: true,
		},
		{
			name:    "empty URL",
			url:     "",
			wantErr: true,
		},
		{
			name:    "too short URL",
			url:     "gs",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBackendFromURL(tt.url)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}