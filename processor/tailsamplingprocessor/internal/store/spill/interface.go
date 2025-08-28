// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
)

// SegmentWriter provides a streaming interface for writing spans to object storage segments.
// Writers are responsible for encoding (Avro/JSONL), compression (zstd), and uploading.
//
// Usage:
//   writer := NewGCSWriter(ctx, client, opts)
//   for _, span := range spans {
//     writer.Append(ctx, span)
//   }
//   segmentRef, err := writer.Flush(ctx)
//   writer.Close(ctx)
//
// Concurrency:
//   - NOT safe for concurrent use
//   - Each writer instance should be used by a single goroutine
type SegmentWriter interface {
	// Append adds a span record to the current segment
	// Returns ErrSegmentFull if segment size limit exceeded
	// Returns ErrWriterClosed if writer has been closed
	Append(ctx context.Context, span SpanRecord) error

	// Flush completes the current segment and uploads to object storage
	// Returns SegmentRef with URL, metadata, and integrity info
	// Returns no-op if no spans have been appended
	// Writer remains open for additional segments after flush
	Flush(ctx context.Context) (SegmentRef, error)

	// Close flushes any pending data and releases all resources
	// After close, writer cannot be used for further operations
	Close(ctx context.Context) error

	// Count returns the number of spans in the current (unflushed) segment
	Count() int

	// Bytes returns the compressed bytes written so far (best effort)
	Bytes() int64

	// IsClosed returns true if the writer has been closed
	IsClosed() bool
}

// SegmentReader provides a streaming interface for reading spans from object storage segments.
// Readers handle decompression and decoding based on the segment's codec.
type SegmentReader interface {
	// OpenSegment prepares the reader for streaming spans from the given segment
	OpenSegment(ctx context.Context, ref SegmentRef) error

	// Next returns the next span record from the segment
	// Returns io.EOF when no more spans are available
	Next(ctx context.Context) (SpanRecord, error)

	// Close releases all resources associated with the reader
	Close(ctx context.Context) error

	// Count returns the total number of spans in the current segment
	Count() int
}

// SegmentFactory creates writers and readers for different backends and codecs
type SegmentFactory interface {
	// NewWriter creates a new segment writer based on the configuration
	NewWriter(ctx context.Context, opts WriterOpts) (SegmentWriter, error)

	// NewReader creates a new segment reader for the specified backend
	NewReader(ctx context.Context, backend Backend) (SegmentReader, error)

	// SupportsBackend returns true if the factory supports the given backend
	SupportsBackend(backend Backend) bool

	// SupportsCodec returns true if the factory supports the given codec
	SupportsCodec(codec Codec) bool
}

// URLBuilder generates object storage URLs following the partitioning scheme
type URLBuilder interface {
	// BuildURL creates a complete object URL for the given parameters
	// Format: gs://{bucket}/{prefix}/tenant={tenant}/date={yyyy-mm-dd}/trace={traceID}/part-{seq}.{extension}
	BuildURL(opts WriterOpts) string

	// ParseURL extracts components from an object storage URL
	ParseURL(url string) (WriterOpts, error)
}