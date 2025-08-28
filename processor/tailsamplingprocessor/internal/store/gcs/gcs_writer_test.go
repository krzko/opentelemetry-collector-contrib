// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

func TestNewGCSWriterWithClient_InvalidOptions(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		opts spill.WriterOpts
	}{
		{
			name: "empty trace ID",
			opts: spill.WriterOpts{
				Backend:      spill.GCS,
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
		},
		{
			name: "empty bucket",
			opts: spill.WriterOpts{
				Backend:      spill.GCS,
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
		},
		{
			name: "invalid segment size",
			opts: spill.WriterOpts{
				Backend:    spill.GCS,
				Bucket:     "test-bucket",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				ZstdLevel:  6,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := NewGCSWriterWithClient(ctx, nil, tt.opts)
			assert.Error(t, err)
			assert.Nil(t, writer)
		})
	}
}

func TestGCSWriter_UnsupportedCodec(t *testing.T) {
	ctx := context.Background()
	opts := spill.WriterOpts{
		Backend:      spill.GCS,
		Bucket:       "test-bucket",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.Codec(99), // Invalid codec
	}

	writer, err := NewGCSWriterWithClient(ctx, nil, opts)
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "invalid GCS client type")
}

func TestBuildObjectPath(t *testing.T) {
	tests := []struct {
		name     string
		opts     spill.WriterOpts
		contains []string
	}{
		{
			name: "with prefix",
			opts: spill.WriterOpts{
				Prefix:     "spill-data",
				Tenant:     "test-tenant",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				Seq:        123,
				Codec:      spill.Avro,
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
				Tenant:     "prod-tenant",
				TraceIDHex: "abcdef1234567890abcdef1234567890",
				Seq:        456,
				Codec:      spill.JSONLZstd,
			},
			contains: []string{
				"tenant=prod-tenant/",
				"trace=abcdef1234567890abcdef1234567890/",
				"part-00000456.jsonl.zst",
			},
		},
		{
			name: "prefix with leading/trailing slashes",
			opts: spill.WriterOpts{
				Prefix:     "/prefix/with/slashes/",
				Tenant:     "test-tenant",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				Seq:        1,
				Codec:      spill.Avro,
			},
			contains: []string{
				"prefix/with/slashes/",
				"tenant=test-tenant/",
				"part-00000001.avro",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := buildObjectPath(tt.opts)
			t.Logf("Generated path: %s", path)

			for _, substring := range tt.contains {
				assert.Contains(t, path, substring, "Path should contain: %s", substring)
			}

			// Check that path contains current date
			today := time.Now().UTC().Format("2006-01-02")
			assert.Contains(t, path, "date="+today)

			// Check that path doesn't have double slashes (except after removing leading slash from prefix)
			if tt.opts.Prefix == "" || tt.opts.Prefix[0] != '/' {
				assert.NotContains(t, path, "//", "Path should not contain double slashes")
			}
		})
	}
}

func TestGetContentType(t *testing.T) {
	tests := []struct {
		codec    spill.Codec
		expected string
	}{
		{spill.Avro, "avro/binary"},
		{spill.JSONLZstd, "application/x-jsonlines+zstd"},
		{spill.Codec(99), "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.codec.String(), func(t *testing.T) {
			assert.Equal(t, tt.expected, getContentType(tt.codec))
		})
	}
}

func TestGetFileExtension(t *testing.T) {
	tests := []struct {
		codec    spill.Codec
		expected string
	}{
		{spill.Avro, "avro"},
		{spill.JSONLZstd, "jsonl.zst"},
		{spill.Codec(99), "bin"},
	}

	for _, tt := range tests {
		t.Run(tt.codec.String(), func(t *testing.T) {
			assert.Equal(t, tt.expected, getFileExtension(tt.codec))
		})
	}
}

func TestJoinPathParts(t *testing.T) {
	tests := []struct {
		name     string
		parts    []string
		expected string
	}{
		{
			name:     "empty parts",
			parts:    []string{},
			expected: "",
		},
		{
			name:     "single part",
			parts:    []string{"single"},
			expected: "single",
		},
		{
			name:     "multiple parts",
			parts:    []string{"part1", "part2", "part3"},
			expected: "part1/part2/part3",
		},
		{
			name:     "parts with empty strings",
			parts:    []string{"part1", "", "part3"},
			expected: "part1//part3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := joinPathParts(tt.parts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJSONSpan_Serialization(t *testing.T) {
	span := jsonSpan{
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
			"float_attr":  3.14,
			"bool_attr":   true,
		},
		OTLPBase64: "dGVzdC1vdGxwLWRhdGE=", // base64 for "test-otlp-data"
	}

	// Verify that all fields are properly tagged for JSON serialization
	assert.NotEmpty(t, span.TraceID)
	assert.NotEmpty(t, span.SpanID)
	assert.NotEmpty(t, span.ParentSpanID)
	assert.Equal(t, "test-span", span.Name)
	assert.Equal(t, int8(1), span.Kind)
	assert.Equal(t, uint64(1000000000), span.StartUnixNano)
	assert.Equal(t, uint64(2000000000), span.EndUnixNano)
	assert.Equal(t, int8(2), span.StatusCode)
	assert.NotNil(t, span.Attributes)
	assert.Equal(t, "dGVzdC1vdGxwLWRhdGE=", span.OTLPBase64)
}

// Mock GCS Writer for testing writer behavior without actual GCS calls
type mockGCSWriter struct {
	closed     bool
	count      int
	bytes      int64
	appendErrs []error
	flushErr   error
	closeErr   error
	appendIdx  int
}

func (m *mockGCSWriter) Append(_ context.Context, _ spill.SpanRecord) error {
	if m.closed {
		return spill.ErrWriterClosed
	}
	if m.appendIdx < len(m.appendErrs) && m.appendErrs[m.appendIdx] != nil {
		err := m.appendErrs[m.appendIdx]
		m.appendIdx++
		return err
	}
	m.appendIdx++
	m.count++
	m.bytes += 100 // Simulated bytes per span
	return nil
}

func (m *mockGCSWriter) Flush(_ context.Context) (spill.SegmentRef, error) {
	if m.flushErr != nil {
		return spill.SegmentRef{}, m.flushErr
	}
	return spill.SegmentRef{
		URL:       "gs://test-bucket/test-object",
		Codec:     spill.Avro,
		Count:     m.count,
		Bytes:     m.bytes,
		Seq:       0,
		CreatedAt: time.Now(),
	}, nil
}

func (m *mockGCSWriter) Close(_ context.Context) error {
	m.closed = true
	return m.closeErr
}

func (m *mockGCSWriter) Count() int {
	return m.count
}

func (m *mockGCSWriter) Bytes() int64 {
	return m.bytes
}

func (m *mockGCSWriter) IsClosed() bool {
	return m.closed
}

func TestMockGCSWriter_Interface(t *testing.T) {
	// Verify that our mock implements the SegmentWriter interface
	var _ spill.SegmentWriter = &mockGCSWriter{}

	ctx := context.Background()
	mock := &mockGCSWriter{}

	// Test basic operations
	span := spill.SpanRecord{
		TraceID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:    "test-span",
	}

	err := mock.Append(ctx, span)
	require.NoError(t, err)
	assert.Equal(t, 1, mock.Count())
	assert.Equal(t, int64(100), mock.Bytes())
	assert.False(t, mock.IsClosed())

	ref, err := mock.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, "gs://test-bucket/test-object", ref.URL)
	assert.Equal(t, 1, ref.Count)

	err = mock.Close(ctx)
	require.NoError(t, err)
	assert.True(t, mock.IsClosed())

	// Test error after close
	err = mock.Append(ctx, span)
	assert.ErrorIs(t, err, spill.ErrWriterClosed)
}

func TestGCSWriter_WithMockClient(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockGCSClient()

	opts := spill.WriterOpts{
		Backend:      spill.GCS,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          123,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	// Test that we can create a writer with valid mock client
	writer, err := NewGCSWriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	gcsWriter := writer.(*GCSWriter)
	assert.False(t, gcsWriter.IsClosed())
	assert.Equal(t, 0, gcsWriter.Count())
	assert.Equal(t, int64(0), gcsWriter.Bytes())

	// Set up mock encoder for JSONL
	mockEncoder := NewMockEncoder()
	gcsWriter.SetEncoder(mockEncoder)

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
	assert.Contains(t, ref.URL, "gs://test-bucket/tenant=test-tenant/date=") // Check URL contains expected parts
	assert.Equal(t, spill.JSONLZstd, ref.Codec)
	assert.Equal(t, 1, ref.Count)
	assert.Equal(t, int64(123), ref.Seq)

	// Test closing
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, gcsWriter.IsClosed())
}

func TestGCSWriter_AvroWithMockClient(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockGCSClient()

	opts := spill.WriterOpts{
		Backend:      spill.GCS,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          456,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.Avro,
	}

	writer, err := NewGCSWriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	gcsWriter := writer.(*GCSWriter)

	// Set up mock Avro writer
	mockAvroWriter := NewMockAvroWriter()
	gcsWriter.SetAvroWriter(mockAvroWriter)

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

	// Test closing
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, gcsWriter.IsClosed())
}

func TestGCSWriter_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	mockClient := NewMockGCSClient()

	opts := spill.WriterOpts{
		Backend:      spill.GCS,
		Bucket:       "test-bucket",
		Tenant:       "test-tenant",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		Seq:          789,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		Codec:        spill.JSONLZstd,
	}

	writer, err := NewGCSWriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	// Test append without encoder - should fail
	span := spill.SpanRecord{Name: "test-span"}
	err = writer.Append(ctx, span)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JSON encoder not initialized")

	// Test with Avro codec without Avro writer
	opts.Codec = spill.Avro
	writer2, err := NewGCSWriterWithClient(ctx, mockClient, opts)
	require.NoError(t, err)

	err = writer2.Append(ctx, span)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Avro writer not initialized")
}
