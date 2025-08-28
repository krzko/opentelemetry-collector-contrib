// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

// GCSClient interface abstracts the GCS storage operations
type GCSClient interface {
	// Bucket returns a bucket handle
	Bucket(name string) GCSBucket
}

// GCSBucket interface abstracts GCS bucket operations
type GCSBucket interface {
	// Object returns an object handle
	Object(name string) GCSObject
}

// GCSObject interface abstracts GCS object operations
type GCSObject interface {
	// NewWriter creates a new writer for the object
	NewWriter(ctx context.Context) GCSObjectWriter
	// NewReader creates a new reader for the object
	NewReader(ctx context.Context) io.ReadCloser
	// Attrs returns the object attributes
	Attrs(ctx context.Context) (*GCSObjectAttrs, error)
}

// GCSObjectWriter interface abstracts GCS object writing operations
type GCSObjectWriter interface {
	io.WriteCloser
	// SetKMSKeyName sets the KMS key for encryption
	SetKMSKeyName(keyName string)
	// SetContentType sets the content type
	SetContentType(contentType string)
}

// GCSObjectAttrs represents GCS object attributes
type GCSObjectAttrs struct {
	Size   int64
	Etag   string
	CRC32C uint32
}

// Encoder interface abstracts compression/encoding operations
type Encoder interface {
	io.WriteCloser
	// Flush flushes any pending data
	Flush() error
}

// AvroWriter interface abstracts Avro writing operations
type AvroWriter interface {
	// Append appends a record
	Append(record map[string]any) error
	// Close closes the writer
	Close() error
}

// GCSWriter implements SegmentWriter interface for Google Cloud Storage
type GCSWriter struct {
	ctx        context.Context
	client     GCSClient
	opts       spill.WriterOpts
	obj        GCSObject
	w          GCSObjectWriter
	encoder    Encoder
	avroWriter AvroWriter
	buf        *bytes.Buffer
	count      int
	wbytes     int64
	closed     bool
	codec      spill.Codec
	jsonEnc    *json.Encoder
	startedAt  time.Time
	timer      *time.Timer
}

// NewGCSWriterWithClient creates a new GCS segment writer with dependency injection
func NewGCSWriterWithClient(ctx context.Context, client interface{}, opts spill.WriterOpts) (spill.SegmentWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid writer options: %w", err)
	}

	// Type assert the client to our interface
	gcsClient, ok := client.(GCSClient)
	if !ok {
		return nil, fmt.Errorf("invalid GCS client type")
	}

	objectName := buildObjectPath(opts)
	obj := gcsClient.Bucket(opts.Bucket).Object(objectName)
	w := obj.NewWriter(ctx)

	// Set CMEK encryption if provided
	if opts.GCSEncryptionKey != "" {
		w.SetKMSKeyName(opts.GCSEncryptionKey)
	}

	// Set content type based on codec
	w.SetContentType(getContentType(opts.Codec))

	gw := &GCSWriter{
		ctx:       ctx,
		client:    gcsClient,
		opts:      opts,
		obj:       obj,
		w:         w,
		buf:       bytes.NewBuffer(nil),
		codec:     opts.Codec,
		startedAt: time.Now().UTC(),
	}

	// Set up flush timer if specified
	if opts.FlushInterval > 0 {
		gw.timer = time.AfterFunc(opts.FlushInterval, func() {
			// Best-effort async flush hint; safe because Flush handles empty
			_, _ = gw.Flush(ctx)
		})
	}

	// Initialize codec-specific components (will be set up via dependency injection)
	// For now, we allow creation without encoders/writers to support testing
	switch opts.Codec {
	case spill.Avro:
		// Avro writer will be injected via SetAvroWriter()
	case spill.JSONLZstd:
		// Encoder will be injected via SetEncoder()
	default:
		w.Close()
		return nil, fmt.Errorf("unsupported codec: %v", opts.Codec)
	}

	return gw, nil
}

// SetEncoder sets the encoder for compression/encoding operations
func (w *GCSWriter) SetEncoder(encoder Encoder) {
	w.encoder = encoder
	if w.codec == spill.JSONLZstd {
		w.jsonEnc = json.NewEncoder(encoder)
	}
}

// SetAvroWriter sets the Avro writer for Avro format operations
func (w *GCSWriter) SetAvroWriter(avroWriter AvroWriter) {
	w.avroWriter = avroWriter
}

// Append adds a span record to the segment
func (w *GCSWriter) Append(ctx context.Context, span spill.SpanRecord) error {
	if w.closed {
		return spill.ErrWriterClosed
	}

	switch w.codec {
	case spill.Avro:
		return w.appendAvro(span)
	case spill.JSONLZstd:
		return w.appendJSONL(span)
	default:
		return fmt.Errorf("unsupported codec: %v", w.codec)
	}
}

// appendAvro appends a span record in Avro format
func (w *GCSWriter) appendAvro(span spill.SpanRecord) error {
	if w.avroWriter == nil {
		return fmt.Errorf("Avro writer not initialized")
	}

	avroRecord := span.ToAvroRecord()
	record := map[string]any{
		"trace_id":        avroRecord.TraceID[:],
		"span_id":         avroRecord.SpanID[:],
		"parent_span_id":  avroRecord.ParentSpanID[:],
		"name":            avroRecord.Name,
		"kind":            avroRecord.Kind,
		"start_unix_nano": avroRecord.StartUnixNano,
		"end_unix_nano":   avroRecord.EndUnixNano,
		"status_code":     avroRecord.StatusCode,
		"attributes":      avroRecord.Attributes,
		"otlp_bytes":      avroRecord.OTLPBytes,
	}

	if err := w.avroWriter.Append(record); err != nil {
		return fmt.Errorf("failed to append Avro record: %w", err)
	}

	w.count++
	return nil
}

// appendJSONL appends a span record in JSONL+zstd format
func (w *GCSWriter) appendJSONL(span spill.SpanRecord) error {
	if w.jsonEnc == nil {
		return fmt.Errorf("JSON encoder not initialized")
	}

	// Create JSONL record with base64-encoded OTLP bytes
	jsonRecord := jsonSpan{
		TraceID:       span.TraceID,
		SpanID:        span.SpanID,
		ParentSpanID:  span.ParentSpanID,
		Name:          span.Name,
		Kind:          span.Kind,
		StartUnixNano: span.StartUnixNano,
		EndUnixNano:   span.EndUnixNano,
		StatusCode:    span.StatusCode,
		Attributes:    span.Attributes,
		OTLPBase64:    base64.StdEncoding.EncodeToString(span.OTLPBytes),
	}

	if err := w.jsonEnc.Encode(&jsonRecord); err != nil {
		return fmt.Errorf("failed to encode JSON record: %w", err)
	}

	w.count++
	return nil
}

// Flush finalizes the current segment and uploads it to GCS
func (w *GCSWriter) Flush(ctx context.Context) (spill.SegmentRef, error) {
	if w.count == 0 {
		return spill.SegmentRef{}, nil
	}

	if w.closed {
		return spill.SegmentRef{}, spill.ErrWriterClosed
	}

	// For GCS, we need to close the writer to complete the upload
	// This means Flush is essentially the same as Close for GCS
	return w.finalizeSegment(ctx)
}

// Close finalizes and closes the writer
func (w *GCSWriter) Close(ctx context.Context) error {
	if w.closed {
		return nil
	}

	w.closed = true

	if w.timer != nil {
		w.timer.Stop()
	}

	_, err := w.finalizeSegment(ctx)
	return err
}

// finalizeSegment completes the segment upload and returns the reference
func (w *GCSWriter) finalizeSegment(ctx context.Context) (spill.SegmentRef, error) {
	if w.count == 0 {
		return spill.SegmentRef{}, nil
	}

	var closeErr error
	switch w.codec {
	case spill.Avro:
		if w.avroWriter != nil {
			closeErr = w.avroWriter.Close()
		}
	case spill.JSONLZstd:
		if w.encoder != nil {
			if err := w.encoder.Close(); err != nil {
				closeErr = err
			}
		}
		if err := w.w.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	default:
		closeErr = fmt.Errorf("unsupported codec: %v", w.codec)
	}

	if closeErr != nil {
		return spill.SegmentRef{}, fmt.Errorf("failed to close writer: %w", closeErr)
	}

	// Get object attributes for size and integrity information
	attrs, err := w.obj.Attrs(ctx)
	if err != nil {
		return spill.SegmentRef{}, fmt.Errorf("failed to get object attributes: %w", err)
	}

	w.wbytes = attrs.Size

	// Build the segment reference
	ref := spill.SegmentRef{
		URL:       fmt.Sprintf("gs://%s/%s", w.opts.Bucket, buildObjectPath(w.opts)),
		Codec:     w.codec,
		Count:     w.count,
		Bytes:     w.wbytes,
		Seq:       w.opts.Seq,
		CreatedAt: w.startedAt,
		ETag:      attrs.Etag,
		CRC32C:    attrs.CRC32C,
	}

	return ref, nil
}

// Count returns the number of spans written
func (w *GCSWriter) Count() int {
	return w.count
}

// Bytes returns the number of bytes written (best effort)
func (w *GCSWriter) Bytes() int64 {
	return w.wbytes
}

// IsClosed returns whether the writer is closed
func (w *GCSWriter) IsClosed() bool {
	return w.closed
}

// buildObjectPath creates the object path following the partitioning scheme
func buildObjectPath(opts spill.WriterOpts) string {
	date := time.Now().UTC().Format("2006-01-02")
	pathParts := make([]string, 0, 4)

	// Add optional prefix
	if opts.Prefix != "" {
		// Clean prefix by removing leading/trailing slashes
		prefix := opts.Prefix
		if prefix[0] == '/' {
			prefix = prefix[1:]
		}
		if len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
			prefix = prefix[:len(prefix)-1]
		}
		if prefix != "" {
			pathParts = append(pathParts, prefix)
		}
	}

	// Add partitioning components
	pathParts = append(pathParts,
		fmt.Sprintf("tenant=%s", opts.Tenant),
		fmt.Sprintf("date=%s", date),
		fmt.Sprintf("trace=%s", opts.TraceIDHex),
		fmt.Sprintf("part-%08d.%s", opts.Seq, getFileExtension(opts.Codec)),
	)

	return joinPathParts(pathParts)
}

// getContentType returns the appropriate content type for the codec
func getContentType(codec spill.Codec) string {
	switch codec {
	case spill.Avro:
		return "avro/binary"
	case spill.JSONLZstd:
		return "application/x-jsonlines+zstd"
	default:
		return "application/octet-stream"
	}
}

// getFileExtension returns the file extension for the codec
func getFileExtension(codec spill.Codec) string {
	switch codec {
	case spill.Avro:
		return "avro"
	case spill.JSONLZstd:
		return "jsonl.zst"
	default:
		return "bin"
	}
}

// joinPathParts joins path parts with '/' separator
func joinPathParts(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}

	// Calculate total length to avoid multiple allocations
	totalLen := len(parts) - 1 // separators
	for _, part := range parts {
		totalLen += len(part)
	}

	result := make([]byte, 0, totalLen)
	for i, part := range parts {
		if i > 0 {
			result = append(result, '/')
		}
		result = append(result, part...)
	}

	return string(result)
}

// jsonSpan represents a span record in JSON format for JSONL serialization
type jsonSpan struct {
	TraceID       [16]byte       `json:"trace_id"`
	SpanID        [8]byte        `json:"span_id"`
	ParentSpanID  [8]byte        `json:"parent_span_id"`
	Name          string         `json:"name"`
	Kind          int8           `json:"kind"`
	StartUnixNano uint64         `json:"start_unix_nano"`
	EndUnixNano   uint64         `json:"end_unix_nano"`
	StatusCode    int8           `json:"status_code"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	OTLPBase64    string         `json:"otlp_b64,omitempty"`
}
