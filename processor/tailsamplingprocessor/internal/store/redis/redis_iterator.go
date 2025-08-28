// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// RedisSpanIterator implements store.SpanIterator for Redis-backed spans
type RedisSpanIterator struct {
	client     RedisClient
	keys       TraceKeys
	logger     *zap.Logger
	batchSize  int
	currentPos int64
	totalSpans int64
	closed     bool
}

// NewRedisSpanIterator creates a new Redis-backed span iterator
func NewRedisSpanIterator(ctx context.Context, client RedisClient, keys TraceKeys, opts store.FetchOptions, logger *zap.Logger) (*RedisSpanIterator, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	
	// Get total span count
	totalSpans, err := client.LLen(ctx, keys.Spans)
	if err != nil {
		return nil, fmt.Errorf("failed to get span count: %w", err)
	}
	
	batchSize := 100 // Default batch size
	if opts.MaxSpans > 0 && opts.MaxSpans < batchSize {
		batchSize = opts.MaxSpans
	}
	
	return &RedisSpanIterator{
		client:     client,
		keys:       keys,
		logger:     logger.With(zap.String("component", "redis-span-iterator")),
		batchSize:  batchSize,
		currentPos: 0,
		totalSpans: totalSpans,
		closed:     false,
	}, nil
}

// Next implements store.SpanIterator
func (it *RedisSpanIterator) Next(ctx context.Context) (store.DecodedSpans, error) {
	if it.closed {
		return nil, fmt.Errorf("iterator is closed")
	}
	
	if it.currentPos >= it.totalSpans {
		return nil, io.EOF
	}
	
	// Calculate the range for this batch
	start := it.currentPos
	end := start + int64(it.batchSize) - 1
	if end >= it.totalSpans {
		end = it.totalSpans - 1
	}
	
	// Fetch spans from Redis LIST
	spanData, err := it.client.LRange(ctx, it.keys.Spans, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch span batch: %w", err)
	}
	
	// Convert to DecodedSpans
	decodedSpans := &RedisDecodedSpans{
		spans: make([]store.SpanRecord, len(spanData)),
	}
	
	for i, data := range spanData {
		spanRecord, err := it.decodeSpanData(data)
		if err != nil {
			it.logger.Warn("Failed to decode span", zap.Error(err))
			continue
		}
		decodedSpans.spans[i] = spanRecord
	}
	
	// Update position
	it.currentPos = end + 1
	
	return decodedSpans, nil
}

// Close implements store.SpanIterator
func (it *RedisSpanIterator) Close() error {
	it.closed = true
	return nil
}

// decodeSpanData converts Redis span data to SpanRecord
func (it *RedisSpanIterator) decodeSpanData(data string) (store.SpanRecord, error) {
	// For now, return a minimal span record with raw data
	// In a full implementation, this would decode OTLP bytes or JSON
	return store.SpanRecord{
		Raw: []byte(data),
	}, nil
}

// RedisDecodedSpans implements store.DecodedSpans for Redis-fetched spans
type RedisDecodedSpans struct {
	spans []store.SpanRecord
}

// Len implements store.DecodedSpans
func (rds *RedisDecodedSpans) Len() int {
	return len(rds.spans)
}

// At implements store.DecodedSpans
func (rds *RedisDecodedSpans) At(i int) store.SpanRecord {
	if i < 0 || i >= len(rds.spans) {
		return store.SpanRecord{}
	}
	return rds.spans[i]
}

// NewRedisDecodedSpans creates a new RedisDecodedSpans from raw span data
func NewRedisDecodedSpans(spanData []string) store.DecodedSpans {
	spans := make([]store.SpanRecord, 0, len(spanData))
	for _, data := range spanData {
		// For now, create minimal span records
		// In a full implementation, this would decode the OTLP bytes
		spans = append(spans, store.SpanRecord{
			Raw: []byte(data),
		})
	}
	return &RedisDecodedSpans{spans: spans}
}

// NewEmptyDecodedSpans creates an empty RedisDecodedSpans
func NewEmptyDecodedSpans() store.DecodedSpans {
	return &RedisDecodedSpans{spans: []store.SpanRecord{}}
}