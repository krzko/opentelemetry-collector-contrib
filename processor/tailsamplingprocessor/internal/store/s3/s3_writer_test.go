// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

func TestNewS3WriterWithClient_InvalidOptions(t *testing.T) {
	ctx := context.Background()
	
	tests := []struct {
		name string
		opts spill.WriterOpts
	}{
		{
			name: "empty trace ID",
			opts: spill.WriterOpts{
				Backend:      spill.S3,
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
		},
		{
			name: "empty bucket",
			opts: spill.WriterOpts{
				Backend:      spill.S3,
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
		},
		{
			name: "invalid segment size",
			opts: spill.WriterOpts{
				Backend:    spill.S3,
				Bucket:     "test-bucket",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				ZstdLevel:  6,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := NewS3WriterWithClient(ctx, nil, tt.opts)
			assert.Error(t, err)
			assert.Nil(t, writer)
		})
	}
}

func TestS3Writer_UnsupportedCodec(t *testing.T) {
	ctx := context.Background()
	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.Codec(99), // Invalid codec
	}

	writer, err := NewS3WriterWithClient(ctx, nil, opts)
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "invalid S3 client type")
}

func TestS3Writer_InvalidClientType(t *testing.T) {
	ctx := context.Background()
	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.Avro,
	}

	// Pass invalid client type (string instead of S3Client)
	writer, err := NewS3WriterWithClient(ctx, "invalid-client", opts)
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "invalid S3 client type")
}

func TestS3Writer_ObjectKeyGeneration(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	tests := []struct {
		name     string
		opts     spill.WriterOpts
		contains []string
	}{
		{
			name: "with prefix",
			opts: spill.WriterOpts{
				Backend:      spill.S3,
				Bucket:       "test-bucket",
				Prefix:       "spill-data",
				Tenant:       "test-tenant",
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				Seq:          123,
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
				Codec:        spill.Avro,
			},
			contains: []string{
				"spill-data/",
				"tenant=test-tenant/",
				"trace=0102030405060708090a0b0c0d0e0f10/",
				"part-00000123.avro",
			},
		},
		{
			name: "without prefix",
			opts: spill.WriterOpts{
				Backend:      spill.S3,
				Bucket:       "test-bucket",
				Tenant:       "prod-tenant",
				TraceIDHex:   "abcdef1234567890abcdef1234567890",
				Seq:          456,
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
				Codec:        spill.JSONLZstd,
			},
			contains: []string{
				"tenant=prod-tenant/",
				"trace=abcdef1234567890abcdef1234567890/",
				"part-00000456.jsonl.zst",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := NewS3WriterWithClient(ctx, mockClient, tt.opts)
			require.NoError(t, err)
			require.NotNil(t, writer)

			s3Writer := writer.(*S3Writer)
			key := s3Writer.key
			t.Logf("Generated key: %s", key)

			for _, substring := range tt.contains {
				assert.Contains(t, key, substring, "Key should contain: %s", substring)
			}

			// Check that key contains current date
			today := time.Now().UTC().Format("2006-01-02")
			assert.Contains(t, key, "date="+today)

			err = writer.Close(ctx)
			require.NoError(t, err)
		})
	}
}

func TestS3Writer_SSEKMSEncryption(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:        spill.S3,
		Bucket:         "test-bucket",
		Tenant:         "test-tenant",
		TraceIDHex:     "0102030405060708090a0b0c0d0e0f10",
		Seq:            1,
		SegmentBytes:   8 << 20,
		ZstdLevel:      6,
		Codec:          spill.JSONLZstd,
		S3SSEKMSKeyID:  "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	s3Writer := writer.(*S3Writer)
	assert.Equal(t, opts.S3SSEKMSKeyID, s3Writer.opts.S3SSEKMSKeyID)

	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestS3Writer_WithMockClient_JSONL(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          123,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	s3Writer := writer.(*S3Writer)
	assert.False(t, s3Writer.IsClosed())
	assert.Equal(t, 0, s3Writer.Count())
	assert.Equal(t, int64(0), s3Writer.Bytes())

	// Set up mock encoder for JSONL
	mockEncoder := NewMockEncoder()
	s3Writer.SetEncoder(mockEncoder)

	// Test appending a span
	span := spill.SpanRecord{
		TraceID:       [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:        [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		ParentSpanID:  [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
		Name:          "test-span",
		Kind:          1,
		StartUnixNano: 1000000000,
		EndUnixNano:   2000000000,
		StatusCode:    2,
		Attributes: map[string]any{
			"string_attr": "value",
			"int_attr":    int64(42),
		},
		OTLPBytes: []byte("test-otlp-data"),
	}

	err = writer.Append(ctx, span)
	require.NoError(t, err)
	assert.Equal(t, 1, writer.Count())

	// Test flushing
	ref, err := writer.Flush(ctx)
	require.NoError(t, err)
	assert.Contains(t, ref.URL, "s3://test-bucket/tenant=test-tenant/date=") // Check URL contains expected parts
	assert.Equal(t, spill.JSONLZstd, ref.Codec)
	assert.Equal(t, 1, ref.Count)
	assert.Equal(t, int64(123), ref.Seq)
	assert.NotEmpty(t, ref.ETag)

	// Verify object was stored in mock S3
	assert.True(t, mockClient.HasObject("test-bucket", s3Writer.key))
	assert.Equal(t, 1, mockClient.GetPutCallCount())

	// Test closing
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, s3Writer.IsClosed())
}

func TestS3Writer_AvroWithMockClient(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          456,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.Avro,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	s3Writer := writer.(*S3Writer)

	// Set up mock Avro writer
	mockAvroWriter := NewMockAvroWriter()
	s3Writer.SetAvroWriter(mockAvroWriter)

	// Test appending spans
	span := spill.SpanRecord{
		TraceID:       [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:        [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:          "avro-test-span",
		Kind:          2,
		StartUnixNano: 2000000000,
		EndUnixNano:   3000000000,
		StatusCode:    1,
	}

	err = writer.Append(ctx, span)
	require.NoError(t, err)
	assert.Equal(t, 1, writer.Count())

	// Verify the record was written to mock Avro writer
	records := mockAvroWriter.GetRecords()
	require.Len(t, records, 1)
	assert.Equal(t, "avro-test-span", records[0]["name"])
	assert.Equal(t, int32(2), records[0]["kind"])
	assert.Equal(t, int64(2000000000), records[0]["start_unix_nano"])

	// Test flushing
	ref, err := writer.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, spill.Avro, ref.Codec)
	assert.Equal(t, 1, ref.Count)

	// Test closing
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, s3Writer.IsClosed())
}

func TestS3Writer_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          789,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	// Test append without encoder - should fail
	span := spill.SpanRecord{Name: "test-span"}
	err = writer.Append(ctx, span)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JSON encoder not initialized")

	// Test with Avro codec without Avro writer
	opts.Codec = spill.Avro
	writer2, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	err = writer2.Append(ctx, span)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Avro writer not initialized")
}

func TestS3Writer_PutObjectError(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          1,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	s3Writer := writer.(*S3Writer)
	mockEncoder := NewMockEncoder()
	s3Writer.SetEncoder(mockEncoder)

	// Set up mock client to return error on PutObject
	mockClient.SetPutError(fmt.Errorf("S3 service unavailable"))

	span := spill.SpanRecord{Name: "test-span"}
	err = writer.Append(ctx, span)
	require.NoError(t, err)

	// Flush should fail due to S3 error
	ref, err := writer.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upload object to S3")
	assert.Contains(t, err.Error(), "S3 service unavailable")
	assert.Equal(t, spill.SegmentRef{}, ref)
}

func TestS3Writer_ClosedWriter(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          1,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	s3Writer := writer.(*S3Writer)
	mockEncoder := NewMockEncoder()
	s3Writer.SetEncoder(mockEncoder)

	// Close the writer
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, s3Writer.IsClosed())

	// Try to append after close - should fail
	span := spill.SpanRecord{Name: "test-span"}
	err = writer.Append(ctx, span)
	assert.ErrorIs(t, err, spill.ErrWriterClosed)

	// Try to flush after close - should fail
	ref, err := writer.Flush(ctx)
	assert.ErrorIs(t, err, spill.ErrWriterClosed)
	assert.Equal(t, spill.SegmentRef{}, ref)
}

func TestS3Writer_EmptyFlush(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockS3Client()

	opts := spill.WriterOpts{
		Backend:      spill.S3,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          1,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewS3WriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	// Flush without any spans should return empty spill.SegmentRef
	ref, err := writer.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, spill.SegmentRef{}, ref)
	assert.Equal(t, 0, mockClient.GetPutCallCount())

	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestTrimQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"etag-value"`, "etag-value"},
		{`"quoted"`, "quoted"},
		{`unquoted`, "unquoted"},
		{`"`, `"`},
		{`""`, ""},
		{`"single`, `"single`},
		{`single"`, `single"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := trimQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}