// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PrefetchOptions configures the prefetcher behavior
type PrefetchOptions struct {
	// MaxConcurrency limits the number of concurrent segment fetches
	MaxConcurrency int

	// BufferSize sets the size of the channel buffer for prefetched spans
	BufferSize int

	// Timeout for individual segment fetch operations
	FetchTimeout time.Duration

	// RetryAttempts for failed segment fetches
	RetryAttempts int

	// RetryDelay between retry attempts
	RetryDelay time.Duration
}

// SetDefaults sets default values for prefetch options
func (opts *PrefetchOptions) SetDefaults() {
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 4
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 1000
	}
	if opts.FetchTimeout <= 0 {
		opts.FetchTimeout = 30 * time.Second
	}
	if opts.RetryAttempts <= 0 {
		opts.RetryAttempts = 3
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 1 * time.Second
	}
}

// Validate checks the prefetch options
func (opts *PrefetchOptions) Validate() error {
	if opts.MaxConcurrency < 1 || opts.MaxConcurrency > 100 {
		return fmt.Errorf("max_concurrency must be between 1 and 100")
	}
	if opts.BufferSize < 1 || opts.BufferSize > 100000 {
		return fmt.Errorf("buffer_size must be between 1 and 100000")
	}
	if opts.FetchTimeout < 1*time.Second || opts.FetchTimeout > 5*time.Minute {
		return fmt.Errorf("fetch_timeout must be between 1s and 5m")
	}
	if opts.RetryAttempts < 0 || opts.RetryAttempts > 10 {
		return fmt.Errorf("retry_attempts must be between 0 and 10")
	}
	if opts.RetryDelay < 0 || opts.RetryDelay > 1*time.Minute {
		return fmt.Errorf("retry_delay must be between 0 and 1m")
	}
	return nil
}

// Prefetcher fetches spans from multiple spill segments in parallel.
// It provides a streaming interface for reading spans from object storage
// with configurable concurrency and buffering.
//
// Responsibilities (following SRP):
// - Coordinate parallel fetching of segments
// - Manage reader lifecycle for each segment
// - Buffer spans for efficient streaming
// - Handle retry logic for failed fetches
type Prefetcher interface {
	// Start begins prefetching spans from the given segments
	// Returns a channel that streams spans as they are fetched
	Start(ctx context.Context, segments []SegmentRef) (<-chan SpanRecord, error)

	// GetErrors returns a channel for receiving fetch errors
	// Non-blocking errors are sent here for monitoring
	GetErrors() <-chan error

	// GetStats returns statistics about the prefetch operation
	GetStats() PrefetchStats

	// Close stops all prefetch operations and releases resources
	Close() error
}

// PrefetchStats provides statistics about prefetch operations
type PrefetchStats struct {
	SegmentsTotal      int
	SegmentsCompleted  int
	SegmentsFailed     int
	SpansFetched       int64
	BytesRead          int64
	ErrorCount         int
	StartTime          time.Time
	EndTime            time.Time
}

// DefaultPrefetcher implements the Prefetcher interface using a worker pool pattern.
// It follows the Dependency Inversion Principle by depending on the SegmentFactory
// abstraction rather than concrete implementations.
type DefaultPrefetcher struct {
	factory SegmentFactory
	opts    PrefetchOptions
	
	// Channels for coordination
	spanChan  chan SpanRecord
	errorChan chan error
	
	// Worker management
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	
	// Statistics
	stats     PrefetchStats
	statsMu   sync.RWMutex
	
	// State management
	closed    bool
	closeMu   sync.Mutex
}

// NewDefaultPrefetcher creates a new prefetcher with the given factory and options
func NewDefaultPrefetcher(factory SegmentFactory, opts PrefetchOptions) (*DefaultPrefetcher, error) {
	opts.SetDefaults()
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid prefetch options: %w", err)
	}
	
	if factory == nil {
		return nil, fmt.Errorf("segment factory is required")
	}
	
	return &DefaultPrefetcher{
		factory:   factory,
		opts:      opts,
		spanChan:  make(chan SpanRecord, opts.BufferSize),
		errorChan: make(chan error, opts.MaxConcurrency),
	}, nil
}

// Start begins prefetching spans from the given segments
func (p *DefaultPrefetcher) Start(ctx context.Context, segments []SegmentRef) (<-chan SpanRecord, error) {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	
	if p.closed {
		return nil, fmt.Errorf("prefetcher is closed")
	}
	
	if len(segments) == 0 {
		close(p.spanChan)
		return p.spanChan, nil
	}
	
	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	
	// Initialize statistics
	p.stats = PrefetchStats{
		SegmentsTotal: len(segments),
		StartTime:     time.Now(),
	}
	
	// Create work queue
	workQueue := make(chan SegmentRef, len(segments))
	for _, segment := range segments {
		workQueue <- segment
	}
	close(workQueue)
	
	// Start worker pool
	workerCount := p.opts.MaxConcurrency
	if workerCount > len(segments) {
		workerCount = len(segments)
	}
	
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.fetchWorker(ctx, workQueue)
	}
	
	// Start coordinator to close span channel when all workers complete
	go p.coordinator()
	
	return p.spanChan, nil
}

// fetchWorker processes segments from the work queue
func (p *DefaultPrefetcher) fetchWorker(ctx context.Context, workQueue <-chan SegmentRef) {
	defer p.wg.Done()
	
	for segment := range workQueue {
		select {
		case <-ctx.Done():
			return
		default:
			p.fetchSegment(ctx, segment)
		}
	}
}

// fetchSegment fetches spans from a single segment with retry logic
func (p *DefaultPrefetcher) fetchSegment(ctx context.Context, segment SegmentRef) {
	var lastErr error
	
	for attempt := 0; attempt <= p.opts.RetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.opts.RetryDelay):
			}
		}
		
		// Create timeout context for this fetch
		fetchCtx, cancel := context.WithTimeout(ctx, p.opts.FetchTimeout)
		err := p.fetchSegmentOnce(fetchCtx, segment)
		cancel()
		
		if err == nil {
			p.updateStats(true, false)
			return
		}
		
		lastErr = err
	}
	
	// All retries failed
	p.updateStats(false, true)
	select {
	case p.errorChan <- fmt.Errorf("failed to fetch segment %s after %d attempts: %w", 
		segment.URL, p.opts.RetryAttempts+1, lastErr):
	default:
		// Error channel full, drop error
	}
}

// fetchSegmentOnce performs a single fetch attempt for a segment
func (p *DefaultPrefetcher) fetchSegmentOnce(ctx context.Context, segment SegmentRef) error {
	// Ensure codec is detected from URL if not already set
	if err := segment.DetectCodec(); err != nil {
		return fmt.Errorf("failed to detect codec for segment %s: %w", segment.URL, err)
	}
	
	// Determine backend from URL
	backend, err := parseBackendFromURL(segment.URL)
	if err != nil {
		return fmt.Errorf("invalid segment URL: %w", err)
	}
	
	// Create reader for the backend
	reader, err := p.factory.NewReader(ctx, backend)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Close(ctx)
	
	// Open the segment
	if err := reader.OpenSegment(ctx, segment); err != nil {
		return fmt.Errorf("failed to open segment: %w", err)
	}
	
	// Stream spans from the segment
	spanCount := 0
	for {
		span, err := reader.Next(ctx)
		if err != nil {
			if err.Error() == "EOF" || err.Error() == "io: EOF" {
				// End of segment reached successfully
				break
			}
			return fmt.Errorf("failed to read span: %w", err)
		}
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.spanChan <- span:
			spanCount++
			p.incrementSpanCount()
		}
	}
	
	// Update bytes read
	p.addBytesRead(segment.Bytes)
	
	return nil
}

// coordinator waits for all workers to complete and closes the span channel
func (p *DefaultPrefetcher) coordinator() {
	p.wg.Wait()
	close(p.spanChan)
	
	// Update end time
	p.statsMu.Lock()
	p.stats.EndTime = time.Now()
	p.statsMu.Unlock()
}

// updateStats updates prefetch statistics
func (p *DefaultPrefetcher) updateStats(completed bool, failed bool) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	
	if completed {
		p.stats.SegmentsCompleted++
	}
	if failed {
		p.stats.SegmentsFailed++
		p.stats.ErrorCount++
	}
}

// incrementSpanCount increments the span count atomically
func (p *DefaultPrefetcher) incrementSpanCount() {
	p.statsMu.Lock()
	p.stats.SpansFetched++
	p.statsMu.Unlock()
}

// addBytesRead adds to the bytes read counter
func (p *DefaultPrefetcher) addBytesRead(bytes int64) {
	p.statsMu.Lock()
	p.stats.BytesRead += bytes
	p.statsMu.Unlock()
}

// GetErrors returns the error channel
func (p *DefaultPrefetcher) GetErrors() <-chan error {
	return p.errorChan
}

// GetStats returns current prefetch statistics
func (p *DefaultPrefetcher) GetStats() PrefetchStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

// Close stops all prefetch operations and releases resources
func (p *DefaultPrefetcher) Close() error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	
	if p.closed {
		return nil
	}
	
	p.closed = true
	
	// Cancel context to stop all workers
	if p.cancel != nil {
		p.cancel()
	}
	
	// Wait for workers to complete
	p.wg.Wait()
	
	// Close error channel
	close(p.errorChan)
	
	return nil
}

// parseBackendFromURL determines the backend from a storage URL
func parseBackendFromURL(url string) (Backend, error) {
	if len(url) < 5 {
		return 0, fmt.Errorf("invalid URL: too short")
	}
	
	switch url[:5] {
	case "gs://":
		return GCS, nil
	case "s3://":
		return S3, nil
	default:
		return 0, fmt.Errorf("unsupported storage backend: %s", url)
	}
}