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
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

// S3Reader implements SegmentReader for Amazon S3.
// It follows the Single Responsibility Principle by focusing solely on
// reading and decoding segments from S3.
type S3Reader struct {
	client  S3Client
	decoder Decoder
	
	// Current segment state
	currentRef    spill.SegmentRef
	currentReader io.ReadCloser
	currentCodec  spill.Codec
	
	// For JSONL format
	jsonlScanner *JSONLScanner
	
	// For Avro format
	avroReader AvroReader
	
	// Statistics
	spanCount int
	closed    bool
}

// Decoder is an interface for decompressing data.
// This follows the Dependency Inversion Principle.
type Decoder interface {
	// NewReader creates a decompression reader
	NewReader(r io.Reader) (io.ReadCloser, error)
}

// AvroReader is an interface for reading Avro OCF files.
// This allows for dependency injection and testing.
type AvroReader interface {
	// HasNext returns true if there are more records
	HasNext() bool
	
	// Next reads the next record into the provided interface
	Next(v interface{}) error
	
	// Close releases resources
	Close() error
}

// JSONLScanner helps read JSONL files line by line
type JSONLScanner struct {
	reader  io.Reader
	buffer  []byte
	scanner *bytes.Buffer
}

// NewJSONLScanner creates a new JSONL scanner
func NewJSONLScanner(reader io.Reader) *JSONLScanner {
	return &JSONLScanner{
		reader:  reader,
		buffer:  make([]byte, 4096),
		scanner: &bytes.Buffer{},
	}
}

// NextLine reads the next line from the JSONL file
func (s *JSONLScanner) NextLine() ([]byte, error) {
	for {
		// Check if we have a complete line in the buffer
		if line := s.scanner.Bytes(); len(line) > 0 {
			if idx := bytes.IndexByte(line, '\n'); idx >= 0 {
				result := make([]byte, idx)
				copy(result, line[:idx])
				s.scanner.Next(idx + 1) // Consume the line including newline
				return result, nil
			}
		}
		
		// Read more data
		n, err := s.reader.Read(s.buffer)
		if n > 0 {
			s.scanner.Write(s.buffer[:n])
		}
		
		if err != nil {
			if err == io.EOF {
				// Return remaining data if any
				if s.scanner.Len() > 0 {
					result := s.scanner.Bytes()
					s.scanner.Reset()
					return result, nil
				}
			}
			return nil, err
		}
	}
}

// NewS3Reader creates a new S3 segment reader
func NewS3Reader(client S3Client) (*S3Reader, error) {
	if client == nil {
		return nil, fmt.Errorf("S3 client is required")
	}
	
	return &S3Reader{
		client: client,
	}, nil
}

// SetDecoder sets the decoder for decompression (dependency injection)
func (r *S3Reader) SetDecoder(decoder Decoder) {
	r.decoder = decoder
}

// SetAvroReader sets the Avro reader (dependency injection)
func (r *S3Reader) SetAvroReader(reader AvroReader) {
	r.avroReader = reader
}

// OpenSegment prepares the reader for streaming spans from the given segment
func (r *S3Reader) OpenSegment(ctx context.Context, ref spill.SegmentRef) error {
	if r.closed {
		return fmt.Errorf("reader is closed")
	}
	
	// Close any existing reader
	if r.currentReader != nil {
		r.currentReader.Close()
		r.currentReader = nil
	}
	
	// Parse the S3 URL
	bucket, key, err := r.parseS3URL(ref.URL)
	if err != nil {
		return fmt.Errorf("invalid S3 URL: %w", err)
	}
	
	// Create GetObject input
	input := &S3GetObjectInput{
		Bucket: bucket,
		Key:    key,
	}
	
	// Get the object from S3
	output, err := r.client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	
	if output.Body == nil {
		return fmt.Errorf("S3 GetObject returned nil body")
	}
	
	r.currentReader = output.Body
	r.currentRef = ref
	r.currentCodec = ref.Codec
	r.spanCount = 0
	
	// Set up decoder based on codec
	switch ref.Codec {
	case spill.Avro:
		if err := r.setupAvroReader(output.Body); err != nil {
			output.Body.Close()
			return fmt.Errorf("failed to setup Avro reader: %w", err)
		}
		
	case spill.JSONLZstd:
		if err := r.setupJSONLReader(output.Body); err != nil {
			output.Body.Close()
			return fmt.Errorf("failed to setup JSONL reader: %w", err)
		}
		
	default:
		output.Body.Close()
		return fmt.Errorf("unsupported codec: %v", ref.Codec)
	}
	
	return nil
}

// setupAvroReader sets up the Avro OCF reader
func (r *S3Reader) setupAvroReader(reader io.Reader) error {
	if r.avroReader == nil {
		return fmt.Errorf("avro reader not configured")
	}
	
	// Avro OCF files are typically already compressed
	// The AvroReader should handle the OCF format including any compression
	return nil
}

// setupJSONLReader sets up the JSONL+zstd reader
func (r *S3Reader) setupJSONLReader(reader io.Reader) error {
	if r.decoder == nil {
		return fmt.Errorf("decoder not configured for JSONL+zstd")
	}
	
	// Create decompression reader
	decompReader, err := r.decoder.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create decompression reader: %w", err)
	}
	
	// Replace the current reader with the decompressed one
	if r.currentReader != nil {
		r.currentReader.Close()
	}
	r.currentReader = decompReader
	
	// Create JSONL scanner
	r.jsonlScanner = NewJSONLScanner(decompReader)
	
	return nil
}

// Next returns the next span record from the segment
func (r *S3Reader) Next(ctx context.Context) (spill.SpanRecord, error) {
	if r.closed {
		return spill.SpanRecord{}, fmt.Errorf("reader is closed")
	}
	
	if r.currentReader == nil {
		return spill.SpanRecord{}, fmt.Errorf("no segment opened")
	}
	
	select {
	case <-ctx.Done():
		return spill.SpanRecord{}, ctx.Err()
	default:
	}
	
	switch r.currentCodec {
	case spill.Avro:
		return r.readAvroRecord()
		
	case spill.JSONLZstd:
		return r.readJSONLRecord()
		
	default:
		return spill.SpanRecord{}, fmt.Errorf("unsupported codec: %v", r.currentCodec)
	}
}

// readAvroRecord reads a single record from an Avro OCF file
func (r *S3Reader) readAvroRecord() (spill.SpanRecord, error) {
	if r.avroReader == nil {
		return spill.SpanRecord{}, fmt.Errorf("avro reader not initialized")
	}
	
	if !r.avroReader.HasNext() {
		return spill.SpanRecord{}, io.EOF
	}
	
	// Read into Avro record structure
	var avroRecord AvroSpanRecord
	if err := r.avroReader.Next(&avroRecord); err != nil {
		return spill.SpanRecord{}, fmt.Errorf("failed to read Avro record: %w", err)
	}
	
	// Convert to SpanRecord
	span := spill.SpanRecord{
		TraceID:       avroRecord.TraceID,
		SpanID:        avroRecord.SpanID,
		ParentSpanID:  avroRecord.ParentSpanID,
		Name:          avroRecord.Name,
		Kind:          avroRecord.Kind,
		StartUnixNano: avroRecord.StartUnixNano,
		EndUnixNano:   avroRecord.EndUnixNano,
		StatusCode:    avroRecord.StatusCode,
		Attributes:    avroRecord.Attributes,
		OTLPBytes:     avroRecord.OTLPBytes,
	}
	
	r.spanCount++
	return span, nil
}

// readJSONLRecord reads a single record from a JSONL file
func (r *S3Reader) readJSONLRecord() (spill.SpanRecord, error) {
	if r.jsonlScanner == nil {
		return spill.SpanRecord{}, fmt.Errorf("JSONL scanner not initialized")
	}
	
	line, err := r.jsonlScanner.NextLine()
	if err != nil {
		return spill.SpanRecord{}, err
	}
	
	if len(line) == 0 {
		return spill.SpanRecord{}, io.EOF
	}
	
	// Parse JSON line
	var jsonRecord JSONSpanRecord
	if err := json.Unmarshal(line, &jsonRecord); err != nil {
		return spill.SpanRecord{}, fmt.Errorf("failed to parse JSON: %w", err)
	}
	
	// Convert to SpanRecord
	span, err := r.jsonToSpanRecord(jsonRecord)
	if err != nil {
		return spill.SpanRecord{}, fmt.Errorf("failed to convert JSON record: %w", err)
	}
	
	r.spanCount++
	return span, nil
}

// jsonToSpanRecord converts a JSON record to SpanRecord
func (r *S3Reader) jsonToSpanRecord(jr JSONSpanRecord) (spill.SpanRecord, error) {
	span := spill.SpanRecord{
		Name:          jr.Name,
		Kind:          jr.Kind,
		StartUnixNano: jr.StartUnixNano,
		EndUnixNano:   jr.EndUnixNano,
		StatusCode:    jr.StatusCode,
		Attributes:    jr.Attributes,
	}
	
	// Decode trace ID
	if traceID, err := decodeHexToBytes16(jr.TraceID); err == nil {
		span.TraceID = traceID
	} else {
		return spill.SpanRecord{}, fmt.Errorf("invalid trace ID: %w", err)
	}
	
	// Decode span ID
	if spanID, err := decodeHexToBytes8(jr.SpanID); err == nil {
		span.SpanID = spanID
	} else {
		return spill.SpanRecord{}, fmt.Errorf("invalid span ID: %w", err)
	}
	
	// Decode parent span ID
	if jr.ParentSpanID != "" {
		if parentID, err := decodeHexToBytes8(jr.ParentSpanID); err == nil {
			span.ParentSpanID = parentID
		}
	}
	
	// Decode OTLP bytes from base64
	if jr.OTLPBytes != "" {
		otlpBytes, err := base64.StdEncoding.DecodeString(jr.OTLPBytes)
		if err != nil {
			return spill.SpanRecord{}, fmt.Errorf("failed to decode OTLP bytes: %w", err)
		}
		span.OTLPBytes = otlpBytes
	}
	
	return span, nil
}

// Close releases all resources associated with the reader
func (r *S3Reader) Close(ctx context.Context) error {
	if r.closed {
		return nil
	}
	
	r.closed = true
	
	var errs []error
	
	// Close current reader
	if r.currentReader != nil {
		if err := r.currentReader.Close(); err != nil {
			errs = append(errs, err)
		}
		r.currentReader = nil
	}
	
	// Close Avro reader if any
	if r.avroReader != nil {
		if err := r.avroReader.Close(); err != nil {
			errs = append(errs, err)
		}
		r.avroReader = nil
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	
	return nil
}

// Count returns the total number of spans read from the current segment
func (r *S3Reader) Count() int {
	return r.spanCount
}

// parseS3URL parses an S3 URL into bucket and key
func (r *S3Reader) parseS3URL(url string) (bucket string, key string, err error) {
	if !strings.HasPrefix(url, "s3://") {
		return "", "", fmt.Errorf("not an S3 URL: %s", url)
	}
	
	path := strings.TrimPrefix(url, "s3://")
	parts := strings.SplitN(path, "/", 2)
	
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid S3 URL format: %s", url)
	}
	
	return parts[0], parts[1], nil
}

// AvroSpanRecord represents the Avro schema for a span
type AvroSpanRecord struct {
	TraceID       [16]byte               `avro:"trace_id"`
	SpanID        [8]byte                `avro:"span_id"`
	ParentSpanID  [8]byte                `avro:"parent_span_id"`
	Name          string                 `avro:"name"`
	Kind          int8                   `avro:"kind"`
	StartUnixNano uint64                 `avro:"start_unix_nano"`
	EndUnixNano   uint64                 `avro:"end_unix_nano"`
	StatusCode    int8                   `avro:"status_code"`
	Attributes    map[string]interface{} `avro:"attributes"`
	OTLPBytes     []byte                 `avro:"otlp_bytes"`
}

// JSONSpanRecord represents the JSON format for a span
type JSONSpanRecord struct {
	TraceID       string                 `json:"trace_id"`
	SpanID        string                 `json:"span_id"`
	ParentSpanID  string                 `json:"parent_span_id,omitempty"`
	Name          string                 `json:"name"`
	Kind          int8                   `json:"kind"`
	StartUnixNano uint64                 `json:"start_unix_nano"`
	EndUnixNano   uint64                 `json:"end_unix_nano"`
	StatusCode    int8                   `json:"status_code"`
	Attributes    map[string]interface{} `json:"attributes,omitempty"`
	OTLPBytes     string                 `json:"otlp_bytes,omitempty"` // Base64 encoded
}

// Helper functions for hex encoding/decoding

func decodeHexToBytes16(hex string) ([16]byte, error) {
	var result [16]byte
	if len(hex) != 32 {
		return result, fmt.Errorf("invalid hex length for 16 bytes: %d", len(hex))
	}
	
	for i := 0; i < 16; i++ {
		b, err := hexToByte(hex[i*2], hex[i*2+1])
		if err != nil {
			return result, err
		}
		result[i] = b
	}
	
	return result, nil
}

func decodeHexToBytes8(hex string) ([8]byte, error) {
	var result [8]byte
	if len(hex) != 16 {
		return result, fmt.Errorf("invalid hex length for 8 bytes: %d", len(hex))
	}
	
	for i := 0; i < 8; i++ {
		b, err := hexToByte(hex[i*2], hex[i*2+1])
		if err != nil {
			return result, err
		}
		result[i] = b
	}
	
	return result, nil
}

func hexToByte(h1, h2 byte) (byte, error) {
	d1, err := hexDigit(h1)
	if err != nil {
		return 0, err
	}
	d2, err := hexDigit(h2)
	if err != nil {
		return 0, err
	}
	return (d1 << 4) | d2, nil
}

func hexDigit(b byte) (byte, error) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', nil
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, nil
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, nil
	default:
		return 0, fmt.Errorf("invalid hex digit: %c", b)
	}
}