// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWriterOpts_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    WriterOpts
		wantErr bool
		errType error
	}{
		{
			name: "valid options",
			opts: WriterOpts{
				Backend:      GCS,
				Bucket:       "test-bucket",
				Tenant:       "test-tenant",
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20, // 8MB
				ZstdLevel:    6,
			},
			wantErr: false,
		},
		{
			name: "empty trace ID",
			opts: WriterOpts{
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
			wantErr: true,
			errType: ErrInvalidTraceID,
		},
		{
			name: "empty bucket",
			opts: WriterOpts{
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
			},
			wantErr: true,
			errType: ErrInvalidBucket,
		},
		{
			name: "invalid segment size",
			opts: WriterOpts{
				Bucket:       "test-bucket",
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 0,
				ZstdLevel:    6,
			},
			wantErr: true,
			errType: ErrInvalidSegmentSize,
		},
		{
			name: "invalid zstd level - too low",
			opts: WriterOpts{
				Bucket:       "test-bucket",
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20,
				ZstdLevel:    0,
			},
			wantErr: true,
			errType: ErrInvalidZstdLevel,
		},
		{
			name: "invalid zstd level - too high",
			opts: WriterOpts{
				Bucket:       "test-bucket",
				TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
				SegmentBytes: 8 << 20,
				ZstdLevel:    23,
			},
			wantErr: true,
			errType: ErrInvalidZstdLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCodec_String(t *testing.T) {
	tests := []struct {
		codec    Codec
		expected string
	}{
		{Avro, "avro"},
		{JSONLZstd, "jsonl.zst"},
		{Codec(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.codec.String())
		})
	}
}

func TestSpanRecord_Duration(t *testing.T) {
	tests := []struct {
		name     string
		span     SpanRecord
		expected uint64
	}{
		{
			name: "normal duration",
			span: SpanRecord{
				StartUnixNano: 1000000000,
				EndUnixNano:   2000000000,
			},
			expected: 1000000000,
		},
		{
			name: "zero duration",
			span: SpanRecord{
				StartUnixNano: 1000000000,
				EndUnixNano:   1000000000,
			},
			expected: 0,
		},
		{
			name: "end before start (invalid)",
			span: SpanRecord{
				StartUnixNano: 2000000000,
				EndUnixNano:   1000000000,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.span.Duration())
		})
	}
}

func TestAvroRecord_Conversion(t *testing.T) {
	// Create test span record
	originalSpan := SpanRecord{
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
		OTLPBytes: []byte("test-otlp-data"),
	}

	// Convert to Avro record
	avroRecord := originalSpan.ToAvroRecord()

	// Verify conversion
	assert.Equal(t, originalSpan.TraceID, avroRecord.TraceID)
	assert.Equal(t, originalSpan.SpanID, avroRecord.SpanID)
	assert.Equal(t, originalSpan.ParentSpanID, avroRecord.ParentSpanID)
	assert.Equal(t, originalSpan.Name, avroRecord.Name)
	assert.Equal(t, int32(originalSpan.Kind), avroRecord.Kind)
	assert.Equal(t, int64(originalSpan.StartUnixNano), avroRecord.StartUnixNano)
	assert.Equal(t, int64(originalSpan.EndUnixNano), avroRecord.EndUnixNano)
	assert.Equal(t, int32(originalSpan.StatusCode), avroRecord.StatusCode)
	assert.Equal(t, originalSpan.Attributes, avroRecord.Attributes)
	assert.Equal(t, originalSpan.OTLPBytes, avroRecord.OTLPBytes)

	// Convert back to span record
	convertedSpan := avroRecord.ToSpanRecord()

	// Verify round-trip conversion
	assert.Equal(t, originalSpan, convertedSpan)
}

func TestValidateAvroSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "valid schema",
			schema:  AvroSchemaV1,
			wantErr: false,
		},
		{
			name:    "empty schema",
			schema:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAvroSchema(tt.schema)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}


func TestSegmentRef_Creation(t *testing.T) {
	createdAt := time.Now()
	ref := SegmentRef{
		URL:       "gs://test-bucket/tenant=test/date=2024-01-01/trace=abcd/part-0.avro",
		Codec:     Avro,
		Count:     100,
		Bytes:     1024,
		Seq:       0,
		CreatedAt: createdAt,
		ETag:      "test-etag",
		CRC32C:    12345,
	}

	assert.Equal(t, "gs://test-bucket/tenant=test/date=2024-01-01/trace=abcd/part-0.avro", ref.URL)
	assert.Equal(t, Avro, ref.Codec)
	assert.Equal(t, 100, ref.Count)
	assert.Equal(t, int64(1024), ref.Bytes)
	assert.Equal(t, int64(0), ref.Seq)
	assert.Equal(t, "test-etag", ref.ETag)
	assert.Equal(t, uint32(12345), ref.CRC32C)
}

func TestStats_Initialization(t *testing.T) {
	stats := Stats{
		SegmentsCreated:   10,
		TotalSpansSpilled: 1000,
		TotalBytesSpilled: 1024000,
		GCSOperations:     5,
		S3Operations:      5,
		AvroSegments:      7,
		JSONLSegments:     3,
		WriteErrors:       1,
		FlushErrors:       0,
	}

	assert.Equal(t, int64(10), stats.SegmentsCreated)
	assert.Equal(t, int64(1000), stats.TotalSpansSpilled)
	assert.Equal(t, int64(1024000), stats.TotalBytesSpilled)
	assert.Equal(t, int64(5), stats.GCSOperations)
	assert.Equal(t, int64(5), stats.S3Operations)
	assert.Equal(t, int64(7), stats.AvroSegments)
	assert.Equal(t, int64(3), stats.JSONLSegments)
	assert.Equal(t, int64(1), stats.WriteErrors)
	assert.Equal(t, int64(0), stats.FlushErrors)
}