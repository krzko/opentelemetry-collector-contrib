// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// MergeIteratorOptions configures the merge iterator behavior
type MergeIteratorOptions struct {
	// BatchSize controls the number of spans returned per Next() call
	BatchSize int

	// HotSpansFirst prioritizes hot spans over spilled spans
	HotSpansFirst bool

	// PrefetchOptions for spilled segments
	PrefetchOptions PrefetchOptions
}

// SetDefaults sets default values for merge iterator options
func (opts *MergeIteratorOptions) SetDefaults() {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	opts.PrefetchOptions.SetDefaults()
}

// Validate checks the merge iterator options
func (opts *MergeIteratorOptions) Validate() error {
	if opts.BatchSize < 1 || opts.BatchSize > 10000 {
		return fmt.Errorf("batch_size must be between 1 and 10000")
	}
	return opts.PrefetchOptions.Validate()
}

// HotSpanIterator provides an interface for iterating over hot spans.
// This abstraction allows the merge iterator to work with different
// hot storage implementations (Redis, memory, etc.)
type HotSpanIterator interface {
	// NextBatch returns the next batch of hot spans
	// Returns io.EOF when no more spans are available
	NextBatch(ctx context.Context, maxBatch int) ([]SpanRecord, error)

	// Close releases resources associated with the iterator
	Close(ctx context.Context) error
}

// MergeIterator combines hot spans and spilled spans into a unified stream.
// It follows the Interface Segregation Principle by providing a simple
// iteration interface while hiding the complexity of merging multiple sources.
type MergeIterator interface {
	// NextBatch returns the next batch of spans from hot and/or spilled storage
	// Returns io.EOF when no more spans are available
	NextBatch(ctx context.Context) ([]SpanRecord, error)

	// GetStats returns statistics about the merge operation
	GetStats() MergeStats

	// Close releases all resources associated with the iterator
	Close(ctx context.Context) error
}

// MergeStats provides statistics about merge operations
type MergeStats struct {
	HotSpansRead     int64
	SpilledSpansRead int64
	BatchesReturned  int
	ErrorCount       int
}

// DefaultMergeIterator implements MergeIterator by combining hot and spilled spans.
// It follows the Single Responsibility Principle by focusing solely on merging
// spans from different sources into a unified stream.
type DefaultMergeIterator struct {
	hotIterator  HotSpanIterator
	prefetcher   Prefetcher
	opts         MergeIteratorOptions
	
	// Channels for spilled spans
	spillChan    <-chan SpanRecord
	spillErrors  <-chan error
	
	// Buffered spans
	hotBuffer    []SpanRecord
	spillBuffer  []SpanRecord
	
	// Statistics
	stats        MergeStats
	statsMu      sync.RWMutex
	
	// State management
	hotDone      bool
	spillDone    bool
	closed       bool
	closeMu      sync.Mutex
}

// NewDefaultMergeIterator creates a merge iterator that combines hot and spilled spans
func NewDefaultMergeIterator(
	hotIterator HotSpanIterator,
	spillSegments []SegmentRef,
	factory SegmentFactory,
	opts MergeIteratorOptions,
) (*DefaultMergeIterator, error) {
	opts.SetDefaults()
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid merge iterator options: %w", err)
	}
	
	iterator := &DefaultMergeIterator{
		hotIterator: hotIterator,
		opts:        opts,
		hotBuffer:   make([]SpanRecord, 0, opts.BatchSize),
		spillBuffer: make([]SpanRecord, 0, opts.BatchSize),
	}
	
	// If no hot iterator, mark hot as done
	if hotIterator == nil {
		iterator.hotDone = true
	}
	
	// Start prefetching spilled segments if any
	if len(spillSegments) > 0 {
		prefetcher, err := NewDefaultPrefetcher(factory, opts.PrefetchOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create prefetcher: %w", err)
		}
		iterator.prefetcher = prefetcher
		
		// Start prefetching in background
		ctx := context.Background()
		spillChan, err := prefetcher.Start(ctx, spillSegments)
		if err != nil {
			prefetcher.Close()
			return nil, fmt.Errorf("failed to start prefetcher: %w", err)
		}
		
		iterator.spillChan = spillChan
		iterator.spillErrors = prefetcher.GetErrors()
	} else {
		iterator.spillDone = true
	}
	
	return iterator, nil
}

// NextBatch returns the next batch of spans from hot and/or spilled storage
func (m *DefaultMergeIterator) NextBatch(ctx context.Context) ([]SpanRecord, error) {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	
	if m.closed {
		return nil, fmt.Errorf("iterator is closed")
	}
	
	// Check if we're done with all sources
	if m.hotDone && m.spillDone {
		return nil, io.EOF
	}
	
	// Prepare result batch
	result := make([]SpanRecord, 0, m.opts.BatchSize)
	
	// Fill batch based on priority
	if m.opts.HotSpansFirst {
		// Try to fill from hot spans first
		result = m.fillFromHot(ctx, result)
		if len(result) < m.opts.BatchSize {
			// Fill remaining from spilled spans
			result = m.fillFromSpill(ctx, result)
		}
	} else {
		// Interleave hot and spilled spans
		for len(result) < m.opts.BatchSize && (!m.hotDone || !m.spillDone) {
			// Add some hot spans
			if !m.hotDone && len(result) < m.opts.BatchSize {
				initialLen := len(result)
				result = m.fillFromHot(ctx, result)
				if len(result) == initialLen {
					// No hot spans added, might be done
					m.checkHotDone(ctx)
				}
			}
			
			// Add some spilled spans
			if !m.spillDone && len(result) < m.opts.BatchSize {
				initialLen := len(result)
				result = m.fillFromSpill(ctx, result)
				if len(result) == initialLen {
					// No spilled spans added, might be done
					m.checkSpillDone()
				}
			}
		}
	}
	
	// Update statistics
	m.statsMu.Lock()
	m.stats.BatchesReturned++
	m.statsMu.Unlock()
	
	// Check for errors from prefetcher
	select {
	case err := <-m.spillErrors:
		if err != nil {
			m.statsMu.Lock()
			m.stats.ErrorCount++
			m.statsMu.Unlock()
			// Log error but continue - we can still return partial results
		}
	default:
	}
	
	if len(result) == 0 {
		return nil, io.EOF
	}
	
	return result, nil
}

// fillFromHot fills the result batch with hot spans
func (m *DefaultMergeIterator) fillFromHot(ctx context.Context, result []SpanRecord) []SpanRecord {
	if m.hotDone {
		return result
	}
	
	needed := m.opts.BatchSize - len(result)
	if needed <= 0 {
		return result
	}
	
	// Refill hot buffer if empty
	if len(m.hotBuffer) == 0 && m.hotIterator != nil {
		spans, err := m.hotIterator.NextBatch(ctx, m.opts.BatchSize)
		if err != nil {
			if err == io.EOF {
				m.hotDone = true
			}
			return result
		}
		m.hotBuffer = spans
	}
	
	// Take spans from hot buffer
	toTake := needed
	if toTake > len(m.hotBuffer) {
		toTake = len(m.hotBuffer)
	}
	
	if toTake > 0 {
		result = append(result, m.hotBuffer[:toTake]...)
		m.hotBuffer = m.hotBuffer[toTake:]
		
		// Update statistics
		m.statsMu.Lock()
		m.stats.HotSpansRead += int64(toTake)
		m.statsMu.Unlock()
	}
	
	return result
}

// fillFromSpill fills the result batch with spilled spans
func (m *DefaultMergeIterator) fillFromSpill(ctx context.Context, result []SpanRecord) []SpanRecord {
	if m.spillDone {
		return result
	}
	
	needed := m.opts.BatchSize - len(result)
	if needed <= 0 {
		return result
	}
	
	// Refill spill buffer if empty
	if len(m.spillBuffer) == 0 && m.spillChan != nil {
	fillLoop:
		for len(m.spillBuffer) < m.opts.BatchSize {
			select {
			case <-ctx.Done():
				return result
			case span, ok := <-m.spillChan:
				if !ok {
					m.spillDone = true
					break fillLoop
				}
				m.spillBuffer = append(m.spillBuffer, span)
			default:
				// No more spans immediately available
				break fillLoop
			}
		}
	}
	
	// Take spans from spill buffer
	toTake := needed
	if toTake > len(m.spillBuffer) {
		toTake = len(m.spillBuffer)
	}
	
	if toTake > 0 {
		result = append(result, m.spillBuffer[:toTake]...)
		m.spillBuffer = m.spillBuffer[toTake:]
		
		// Update statistics
		m.statsMu.Lock()
		m.stats.SpilledSpansRead += int64(toTake)
		m.statsMu.Unlock()
	}
	
	return result
}

// checkHotDone checks if hot iterator is exhausted
func (m *DefaultMergeIterator) checkHotDone(ctx context.Context) {
	if m.hotDone || m.hotIterator == nil {
		return
	}
	
	// Try to get one more batch
	spans, err := m.hotIterator.NextBatch(ctx, 1)
	if err == io.EOF || len(spans) == 0 {
		m.hotDone = true
	} else if len(spans) > 0 {
		// Put spans back in buffer
		m.hotBuffer = append(m.hotBuffer, spans...)
	}
}

// checkSpillDone checks if spill channel is exhausted
func (m *DefaultMergeIterator) checkSpillDone() {
	if m.spillDone || m.spillChan == nil {
		return
	}
	
	// Check if channel is closed
	select {
	case span, ok := <-m.spillChan:
		if !ok {
			m.spillDone = true
		} else {
			// Put span back in buffer
			m.spillBuffer = append(m.spillBuffer, span)
		}
	default:
		// Channel not closed but no spans available
	}
}

// GetStats returns current merge statistics
func (m *DefaultMergeIterator) GetStats() MergeStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	return m.stats
}

// Close releases all resources associated with the iterator
func (m *DefaultMergeIterator) Close(ctx context.Context) error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	
	if m.closed {
		return nil
	}
	
	m.closed = true
	
	var errs []error
	
	// Close hot iterator
	if m.hotIterator != nil {
		if err := m.hotIterator.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to close hot iterator: %w", err))
		}
	}
	
	// Close prefetcher
	if m.prefetcher != nil {
		if err := m.prefetcher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close prefetcher: %w", err))
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	
	return nil
}