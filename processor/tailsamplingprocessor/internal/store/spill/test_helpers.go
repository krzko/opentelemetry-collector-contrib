// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// MockSegmentReader implements SegmentReader for testing
type MockSegmentReader struct {
	segments     map[string][]SpanRecord
	currentSpans []SpanRecord
	currentIdx   int
	failAfter    int
	failCount    int
	openDelay    time.Duration
	readDelay    time.Duration
	closed       bool
	mu           sync.Mutex
}

func NewMockSegmentReader() *MockSegmentReader {
	return &MockSegmentReader{
		segments:  make(map[string][]SpanRecord),
		failAfter: -1, // Default to never fail
	}
}

func (r *MockSegmentReader) AddSegment(url string, spans []SpanRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segments[url] = spans
}

func (r *MockSegmentReader) OpenSegment(ctx context.Context, ref SegmentRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.openDelay > 0 {
		select {
		case <-time.After(r.openDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	spans, ok := r.segments[ref.URL]
	if !ok {
		return fmt.Errorf("segment not found: %s", ref.URL)
	}

	r.currentSpans = spans
	r.currentIdx = 0
	return nil
}

func (r *MockSegmentReader) Next(ctx context.Context) (SpanRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readDelay > 0 {
		select {
		case <-time.After(r.readDelay):
		case <-ctx.Done():
			return SpanRecord{}, ctx.Err()
		}
	}

	// failAfter: -1 means never fail, 0 means always fail, > 0 means fail after N reads
	if r.failAfter >= 0 && r.failCount >= r.failAfter {
		return SpanRecord{}, fmt.Errorf("mock read failure")
	}

	if r.currentIdx >= len(r.currentSpans) {
		return SpanRecord{}, io.EOF
	}

	span := r.currentSpans[r.currentIdx]
	r.currentIdx++
	r.failCount++
	return span, nil
}

func (r *MockSegmentReader) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *MockSegmentReader) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.currentSpans)
}

// MockSegmentFactory implements SegmentFactory for testing
type MockSegmentFactory struct {
	reader       *MockSegmentReader
	createErrors map[Backend]error
	readerCount  int32
}

func NewMockSegmentFactory(reader *MockSegmentReader) *MockSegmentFactory {
	return &MockSegmentFactory{
		reader:       reader,
		createErrors: make(map[Backend]error),
	}
}

func (f *MockSegmentFactory) NewWriter(ctx context.Context, opts WriterOpts) (SegmentWriter, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *MockSegmentFactory) NewReader(ctx context.Context, backend Backend) (SegmentReader, error) {
	atomic.AddInt32(&f.readerCount, 1)
	
	if err, exists := f.createErrors[backend]; exists {
		return nil, err
	}
	
	// Create a new reader instance that shares the segments but has its own state
	// This prevents race conditions when multiple goroutines use readers concurrently
	newReader := &MockSegmentReader{
		segments:  f.reader.segments, // Share the segments map (read-only after setup)
		failAfter: f.reader.failAfter,
		openDelay: f.reader.openDelay,
		readDelay: f.reader.readDelay,
		mu:        sync.Mutex{}, // Each reader needs its own mutex
	}
	
	return newReader, nil
}

func (f *MockSegmentFactory) SupportsBackend(backend Backend) bool {
	return backend == GCS || backend == S3
}

func (f *MockSegmentFactory) SupportsCodec(codec Codec) bool {
	return codec == Avro || codec == JSONLZstd
}

func (f *MockSegmentFactory) GetReaderCount() int {
	return int(atomic.LoadInt32(&f.readerCount))
}

// MockHotSpanIterator implements HotSpanIterator for testing
type MockHotSpanIterator struct {
	spans      []SpanRecord
	batchIndex int
	closed     bool
	failAfter  int
	failCount  int
	delay      time.Duration
	mu         sync.Mutex
}

func NewMockHotSpanIterator(spans []SpanRecord) *MockHotSpanIterator {
	return &MockHotSpanIterator{
		spans: spans,
	}
}

func (i *MockHotSpanIterator) NextBatch(ctx context.Context, maxBatch int) ([]SpanRecord, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.delay > 0 {
		select {
		case <-time.After(i.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if i.failAfter > 0 && i.failCount >= i.failAfter {
		return nil, fmt.Errorf("mock hot iterator failure")
	}
	i.failCount++

	if i.batchIndex >= len(i.spans) {
		return nil, io.EOF
	}

	endIndex := i.batchIndex + maxBatch
	if endIndex > len(i.spans) {
		endIndex = len(i.spans)
	}

	batch := i.spans[i.batchIndex:endIndex]
	i.batchIndex = endIndex
	return batch, nil
}

func (i *MockHotSpanIterator) Close(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}