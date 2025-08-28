// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"time"
)

// Backend represents the cloud storage backend type
type Backend int8

const (
	GCS Backend = iota
	S3
)

// Codec represents the storage format for spilled segments
type Codec int8

const (
	Avro Codec = iota
	JSONLZstd
)

// String returns the string representation of the codec
func (c Codec) String() string {
	switch c {
	case Avro:
		return "avro"
	case JSONLZstd:
		return "jsonl.zst"
	default:
		return "unknown"
	}
}

// ParseCodecFromString parses a codec from a string representation
func ParseCodecFromString(s string) (Codec, error) {
	switch s {
	case "avro":
		return Avro, nil
	case "jsonl.zst", "jsonl+zstd":
		return JSONLZstd, nil
	default:
		return 0, ErrUnsupportedCodec
	}
}

// SegmentRef represents a reference to a spilled segment in object storage
type SegmentRef struct {
	URL       string    // gs://... or s3://...
	Codec     Codec     // Avro or JSONLZstd
	Count     int       // number of spans in segment
	Bytes     int64     // compressed bytes
	Seq       int64     // part sequence number
	CreatedAt time.Time // UTC timestamp
	ETag      string    // S3/GCS etag for integrity
	CRC32C    uint32    // GCS CRC32C if available
}

// DetectCodec attempts to detect the codec from the URL extension if not already set
func (ref *SegmentRef) DetectCodec() error {
	// If codec is already set and valid, return success
	if ref.Codec == Avro || ref.Codec == JSONLZstd {
		return nil
	}
	
	// Try to detect from URL extension
	url := ref.URL
	if len(url) == 0 {
		return ErrInvalidURL
	}
	
	// Check file extension
	if len(url) > 5 {
		if url[len(url)-5:] == ".avro" {
			ref.Codec = Avro
			return nil
		}
	}
	
	if len(url) > 10 {
		if url[len(url)-10:] == ".jsonl.zst" {
			ref.Codec = JSONLZstd
			return nil
		}
	}
	
	return ErrUnsupportedCodec
}

// WriterOpts contains configuration for creating segment writers
type WriterOpts struct {
	Backend        Backend
	Bucket         string
	Prefix         string        // optional; no leading slash
	Tenant         string
	TraceIDHex     string        // 32 hex chars
	Seq            int64         // allocated part number (from Redis)
	Codec          Codec         // Avro or JSONLZstd
	SegmentBytes   int64         // hard cap; writer flushes on exceed (e.g., 8<<20)
	FlushInterval  time.Duration // safety timer; flush if no writes for N
	ZstdLevel      int           // default 6..8
	
	// Encryption / KMS (optional)
	GCSEncryptionKey string // CMEK resource id (optional)
	S3SSEKMSKeyID    string // KMS key id (optional)
}

// Validate checks if the WriterOpts are valid
func (opts WriterOpts) Validate() error {
	if opts.TraceIDHex == "" {
		return ErrInvalidTraceID
	}
	if opts.Bucket == "" {
		return ErrInvalidBucket
	}
	if opts.SegmentBytes <= 0 {
		return ErrInvalidSegmentSize
	}
	if opts.ZstdLevel < 1 || opts.ZstdLevel > 22 {
		return ErrInvalidZstdLevel
	}
	return nil
}

// SpanRecord represents a minimal span representation for spill storage
// This aligns with the Avro schema and provides all fields needed for evaluation
type SpanRecord struct {
	TraceID       [16]byte           // 16-byte trace ID
	SpanID        [8]byte            // 8-byte span ID
	ParentSpanID  [8]byte            // 8-byte parent span ID
	Name          string             // span name
	Kind          int8               // span kind
	StartUnixNano uint64             // start timestamp in nanoseconds
	EndUnixNano   uint64             // end timestamp in nanoseconds
	StatusCode    int8               // status code
	Attributes    map[string]any     // attributes (string, int64, float64, bool)
	OTLPBytes     []byte             // raw OTLP span bytes for fidelity
}

// Duration returns the span duration in nanoseconds
func (s SpanRecord) Duration() uint64 {
	if s.EndUnixNano > s.StartUnixNano {
		return s.EndUnixNano - s.StartUnixNano
	}
	return 0
}

// Stats contains statistics about spill operations
type Stats struct {
	SegmentsCreated     int64
	TotalSpansSpilled   int64
	TotalBytesSpilled   int64
	GCSOperations       int64
	S3Operations        int64
	AvroSegments        int64
	JSONLSegments       int64
	WriteErrors         int64
	FlushErrors         int64
}