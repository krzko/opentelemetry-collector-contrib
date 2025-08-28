// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"time"
)

// TraceID represents a 16-byte trace identifier (avoiding import issues)
type TraceID [16]byte

// Hex returns the hex string representation of the trace ID
func (tid TraceID) Hex() string {
	return fmt.Sprintf("%032x", [16]byte(tid))
}

// Tenant represents a tenant identifier
type Tenant string

// EncodedSpans represents encoded span data (simplified interface)
type EncodedSpans interface {
	// GetOTLPBytes returns the raw OTLP protobuf bytes
	GetOTLPBytes() []byte
	// GetNativeBytes returns the native encoded bytes
	GetNativeBytes() []byte
}

// SpillSegmentRef locates a segment in object storage (matching store package)
type SpillSegmentRef struct {
	URL        string     // gs://... or s3://...
	Codec      SpillCodec // Avro or JSONL_ZSTD
	FirstIndex int        // first appended span index in this segment
	Count      int        // number of spans in segment
	Bytes      int64
	CreatedAt  time.Time
}

// SpillCodec enumerates on-disk formats (matching store package)
type SpillCodec int8

const (
	SpillCodecAvro SpillCodec = iota
	SpillCodecJSONLZstd
)

// SegmentManager coordinates between storage systems and object storage for spill operations.
//
// Responsibilities:
// - Coordination of spill operations between storage systems and object storage
// - Management of segment lifecycle from creation to completion
// - Type conversion between different storage system interfaces
type SegmentManager interface {
	// SpillTrace writes trace spans to object storage and returns segment reference
	// This is the main entry point for spill operations
	SpillTrace(ctx context.Context, traceID TraceID, tenant Tenant, spans EncodedSpans) (SpillSegmentRef, error)

	// GetSpillConfig returns the current spill configuration
	GetSpillConfig() SpillConfig

	// Close releases all resources
	Close(ctx context.Context) error
}

// SpillConfig contains configuration for spill operations
type SpillConfig struct {
	// Object storage configuration
	Backend       Backend
	Bucket        string
	Prefix        string
	Codec         Codec
	SegmentBytes  int64
	FlushInterval time.Duration
	ZstdLevel     int

	// Encryption configuration
	GCSEncryptionKey string
	S3SSEKMSKeyID    string

	// Spill trigger thresholds
	MaxSpansPerTrace int64
	MaxBytesPerTrace int64
	SpillEnabled     bool
}

// Validate checks if the SpillConfig is valid
func (c SpillConfig) Validate() error {
	if !c.SpillEnabled {
		return nil // Skip validation if spill is disabled
	}

	if c.Bucket == "" {
		return ErrInvalidBucket
	}
	if c.SegmentBytes <= 0 {
		return ErrInvalidSegmentSize
	}
	if c.ZstdLevel < 1 || c.ZstdLevel > 22 {
		return fmt.Errorf("invalid zstd level: %d (must be 1-22)", c.ZstdLevel)
	}

	return nil
}

// DefaultSegmentManager implements SegmentManager interface
type DefaultSegmentManager struct {
	config     SpillConfig
	factory    SegmentFactory
	urlBuilder URLBuilder
}

// NewSegmentManager creates a new segment manager with the given configuration
func NewSegmentManager(config SpillConfig, factory SegmentFactory) (SegmentManager, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid spill config: %w", err)
	}

	urlBuilder := NewURLBuilder()

	return &DefaultSegmentManager{
		config:     config,
		factory:    factory,
		urlBuilder: urlBuilder,
	}, nil
}

// SpillTrace implements SegmentManager interface
func (sm *DefaultSegmentManager) SpillTrace(ctx context.Context, traceID TraceID, tenant Tenant, spans EncodedSpans) (SpillSegmentRef, error) {
	if !sm.config.SpillEnabled {
		return SpillSegmentRef{}, fmt.Errorf("spill is disabled")
	}

	// For now, create a simple sequence number (in real implementation, this would come from external coordination)
	seq := time.Now().UnixNano() % 1000000

	// Create writer options
	opts := WriterOpts{
		Backend:          sm.config.Backend,
		Bucket:           sm.config.Bucket,
		Prefix:           sm.config.Prefix,
		Tenant:           string(tenant),
		TraceIDHex:       traceID.Hex(),
		Seq:              seq,
		Codec:            sm.config.Codec,
		SegmentBytes:     sm.config.SegmentBytes,
		FlushInterval:    sm.config.FlushInterval,
		ZstdLevel:        sm.config.ZstdLevel,
		GCSEncryptionKey: sm.config.GCSEncryptionKey,
		S3SSEKMSKeyID:    sm.config.S3SSEKMSKeyID,
	}

	// Create segment writer
	writer, err := sm.factory.NewWriter(ctx, opts)
	if err != nil {
		return SpillSegmentRef{}, fmt.Errorf("failed to create segment writer: %w", err)
	}
	defer writer.Close(ctx)

	// For now, create a simple span record from the encoded spans
	// In a real implementation, this would properly decode the EncodedSpans
	// and convert them to SpanRecord format for object storage
	spanRecord := SpanRecord{
		TraceID:    [16]byte(traceID),
		Name:       "spilled-trace",
		Attributes: make(map[string]any),
		OTLPBytes:  spans.GetOTLPBytes(),
	}

	// Write span to segment
	if err := writer.Append(ctx, spanRecord); err != nil {
		return SpillSegmentRef{}, fmt.Errorf("failed to append span to segment: %w", err)
	}

	// Flush the segment to object storage
	segmentRef, err := writer.Flush(ctx)
	if err != nil {
		return SpillSegmentRef{}, fmt.Errorf("failed to flush segment: %w", err)
	}

	// Convert spill.SegmentRef to SpillSegmentRef for interface compatibility
	spillRef := SpillSegmentRef{
		URL:        segmentRef.URL,
		Codec:      convertCodec(segmentRef.Codec),
		FirstIndex: 0,
		Count:      1, // simplified for now
		Bytes:      segmentRef.Bytes,
		CreatedAt:  segmentRef.CreatedAt,
	}

	return spillRef, nil
}

// GetSpillConfig implements SegmentManager interface
func (sm *DefaultSegmentManager) GetSpillConfig() SpillConfig {
	return sm.config
}

// Close implements SegmentManager interface
func (sm *DefaultSegmentManager) Close(ctx context.Context) error {
	// Nothing to close for now, but provides extension point
	return nil
}

// convertCodec converts spill.Codec to SpillCodec for interface compatibility
func convertCodec(codec Codec) SpillCodec {
	switch codec {
	case Avro:
		return SpillCodecAvro
	case JSONLZstd:
		return SpillCodecJSONLZstd
	default:
		return SpillCodecAvro // default fallback
	}
}
