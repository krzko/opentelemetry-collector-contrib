// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3

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

// S3Client interface abstracts the S3 storage operations
type S3Client interface {
	// PutObject uploads an object to S3
	PutObject(ctx context.Context, input *S3PutObjectInput) (*S3PutObjectOutput, error)
	// HeadObject retrieves object metadata from S3
	HeadObject(ctx context.Context, input *S3HeadObjectInput) (*S3HeadObjectOutput, error)
	// GetObject retrieves an object from S3
	GetObject(ctx context.Context, input *S3GetObjectInput) (*S3GetObjectOutput, error)
}

// S3PutObjectInput represents the input for S3 PutObject operation
type S3PutObjectInput struct {
	Bucket                string
	Key                   string
	Body                  io.Reader
	ContentType           string
	ServerSideEncryption  string
	SSEKMSKeyID           string
}

// S3PutObjectOutput represents the output from S3 PutObject operation
type S3PutObjectOutput struct {
	ETag string
}

// S3HeadObjectInput represents the input for S3 HeadObject operation
type S3HeadObjectInput struct {
	Bucket string
	Key    string
}

// S3HeadObjectOutput represents the output from S3 HeadObject operation
type S3HeadObjectOutput struct {
	ContentLength int64
	ETag          string
}

// S3GetObjectInput represents the input for S3 GetObject operations
type S3GetObjectInput struct {
	Bucket string
	Key    string
}

// S3GetObjectOutput represents the output from S3 GetObject operations
type S3GetObjectOutput struct {
	Body         io.ReadCloser
	ContentType  *string
	ETag         *string
	ContentLength *int64
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

// S3Writer implements spill.SegmentWriter interface for Amazon S3
type S3Writer struct {
	ctx       context.Context
	client    S3Client
	opts      spill.WriterOpts
	buf       *bytes.Buffer
	encoder   Encoder
	avroWriter AvroWriter
	count     int
	wbytes    int64
	closed    bool
	codec     spill.Codec
	jsonEnc   *json.Encoder
	startedAt time.Time
	timer     *time.Timer
	key       string
}

// NewS3WriterWithClient creates a new S3 segment writer with dependency injection
func NewS3WriterWithClient(ctx context.Context, client any, opts spill.WriterOpts) (spill.SegmentWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid writer options: %w", err)
	}

	// Type assert the client to our interface
	s3Client, ok := client.(S3Client)
	if !ok {
		return nil, fmt.Errorf("invalid S3 client type")
	}

	objectKey := buildObjectPath(opts)

	sw := &S3Writer{
		ctx:       ctx,
		client:    s3Client,
		opts:      opts,
		buf:       bytes.NewBuffer(make([]byte, 0, opts.SegmentBytes*2)), // pre-size buffer
		codec:     opts.Codec,
		startedAt: time.Now().UTC(),
		key:       objectKey,
	}

	// Set up flush timer if specified
	if opts.FlushInterval > 0 {
		sw.timer = time.AfterFunc(opts.FlushInterval, func() {
			// Best-effort async flush hint; safe because Flush handles empty
			_, _ = sw.Flush(ctx)
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
		return nil, fmt.Errorf("unsupported codec: %v", opts.Codec)
	}

	return sw, nil
}

// SetEncoder sets the encoder for compression/encoding operations
// For S3Writer, we need to create a pipe so the encoder writes to our buffer
func (w *S3Writer) SetEncoder(encoder Encoder) {
	w.encoder = encoder
	if w.codec == spill.JSONLZstd {
		// Create a JSON encoder that writes to our buffer for testing
		// In production, the encoder would be configured to write to our buffer
		w.jsonEnc = json.NewEncoder(w.buf)
	}
}

// SetAvroWriter sets the Avro writer for Avro format operations  
// For S3Writer, we store the writer which should already be configured to write to our buffer
func (w *S3Writer) SetAvroWriter(avroWriter AvroWriter) {
	w.avroWriter = avroWriter
}

// Append adds a span record to the segment
func (w *S3Writer) Append(ctx context.Context, span spill.SpanRecord) error {
	if w.closed {
		return spill.ErrWriterClosed
	}

	switch w.codec {
	case spill.Avro:
		return w.appendAvro(ctx, span)
	case spill.JSONLZstd:
		return w.appendJSONL(ctx, span)
	default:
		return fmt.Errorf("unsupported codec: %v", w.codec)
	}
}

// appendAvro appends a span record in Avro format
func (w *S3Writer) appendAvro(ctx context.Context, span spill.SpanRecord) error {
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

	// Optional: proactive flush if buffer exceeds SegmentBytes
	if int64(w.buf.Len()) >= w.opts.SegmentBytes {
		_, err := w.Flush(ctx)
		return err
	}

	return nil
}

// appendJSONL appends a span record in JSONL+zstd format
func (w *S3Writer) appendJSONL(ctx context.Context, span spill.SpanRecord) error {
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

	// Optional: proactive flush if buffer exceeds SegmentBytes
	if int64(w.buf.Len()) >= w.opts.SegmentBytes {
		_, err := w.Flush(ctx)
		return err
	}

	return nil
}

// Flush finalizes the current segment and uploads it to S3
func (w *S3Writer) Flush(ctx context.Context) (spill.SegmentRef, error) {
	if w.closed {
		return spill.SegmentRef{}, spill.ErrWriterClosed
	}

	if w.count == 0 {
		return spill.SegmentRef{}, nil
	}

	// Ensure encoders are flushed to buffer (for JSONL+zstd)
	switch w.codec {
	case spill.Avro:
		// For Avro, the OCF writer manages the format internally
		// We don't close it here as we might append more records
	case spill.JSONLZstd:
		if w.encoder != nil {
			if err := w.encoder.Flush(); err != nil {
				return spill.SegmentRef{}, fmt.Errorf("failed to flush encoder: %w", err)
			}
		}
	}

	// Prepare S3 PutObject input
	body := bytes.NewReader(w.buf.Bytes())
	putInput := &S3PutObjectInput{
		Bucket:      w.opts.Bucket,
		Key:         w.key,
		Body:        body,
		ContentType: getContentType(w.codec),
	}

	// Set SSE-KMS encryption if provided
	if w.opts.S3SSEKMSKeyID != "" {
		putInput.ServerSideEncryption = "aws:kms"
		putInput.SSEKMSKeyID = w.opts.S3SSEKMSKeyID
	}

	// Upload to S3
	output, err := w.client.PutObject(ctx, putInput)
	if err != nil {
		return spill.SegmentRef{}, fmt.Errorf("failed to upload object to S3: %w", err)
	}

	w.wbytes = int64(w.buf.Len())

	// Build the segment reference
	ref := spill.SegmentRef{
		URL:       fmt.Sprintf("s3://%s/%s", w.opts.Bucket, w.key),
		Codec:     w.codec,
		Count:     w.count,
		Bytes:     w.wbytes,
		Seq:       w.opts.Seq,
		CreatedAt: w.startedAt,
	}

	if output.ETag != "" {
		ref.ETag = trimQuotes(output.ETag)
	}

	return ref, nil
}

// Close finalizes and closes the writer
func (w *S3Writer) Close(ctx context.Context) error {
	if w.closed {
		return nil
	}

	if w.timer != nil {
		w.timer.Stop()
	}

	// Final flush if not yet flushed
	var err error
	if w.count > 0 {
		_, err = w.Flush(ctx)
	}

	// Mark as closed after flush
	w.closed = true
	return err
}

// Count returns the number of spans written
func (w *S3Writer) Count() int {
	return w.count
}

// Bytes returns the number of bytes written (best effort)
func (w *S3Writer) Bytes() int64 {
	return w.wbytes
}

// IsClosed returns whether the writer is closed
func (w *S3Writer) IsClosed() bool {
	return w.closed
}

// trimQuotes removes surrounding quotes from S3 ETags
func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
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