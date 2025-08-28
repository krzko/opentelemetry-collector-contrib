// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockEncodedSpans implements EncodedSpans interface for testing
type MockEncodedSpans struct {
	otlpBytes   []byte
	nativeBytes []byte
}

func (m *MockEncodedSpans) GetOTLPBytes() []byte {
	return m.otlpBytes
}

func (m *MockEncodedSpans) GetNativeBytes() []byte {
	return m.nativeBytes
}

// MockWriterSegmentFactory implements SegmentFactory interface for testing segment manager
type MockWriterSegmentFactory struct{}

func (m *MockWriterSegmentFactory) NewWriter(ctx context.Context, opts WriterOpts) (SegmentWriter, error) {
	return NewMockSegmentWriter(opts), nil
}

func (m *MockWriterSegmentFactory) NewReader(ctx context.Context, backend Backend) (SegmentReader, error) {
	return nil, ErrReaderNotImplemented
}

func (m *MockWriterSegmentFactory) SupportsBackend(backend Backend) bool {
	return backend == GCS || backend == S3
}

func (m *MockWriterSegmentFactory) SupportsCodec(codec Codec) bool {
	return codec == Avro || codec == JSONLZstd
}

// MockSegmentWriter implements SegmentWriter interface for testing
type MockSegmentWriter struct {
	opts      WriterOpts
	records   []SpanRecord
	closed    bool
	flushed   bool
	segmentRef SegmentRef
}

func NewMockSegmentWriter(opts WriterOpts) *MockSegmentWriter {
	return &MockSegmentWriter{
		opts:    opts,
		records: make([]SpanRecord, 0),
		segmentRef: SegmentRef{
			URL:       "mock://bucket/path/to/segment",
			Codec:     opts.Codec,
			Count:     0,
			Bytes:     1024,
			Seq:       opts.Seq,
			CreatedAt: time.Now().UTC(),
			ETag:      "mock-etag",
		},
	}
}

func (m *MockSegmentWriter) Append(ctx context.Context, span SpanRecord) error {
	if m.closed {
		return ErrWriterClosed
	}
	m.records = append(m.records, span)
	m.segmentRef.Count = len(m.records)
	return nil
}

func (m *MockSegmentWriter) Flush(ctx context.Context) (SegmentRef, error) {
	if m.closed {
		return SegmentRef{}, ErrWriterClosed
	}
	if len(m.records) == 0 {
		return SegmentRef{}, nil
	}
	m.flushed = true
	return m.segmentRef, nil
}

func (m *MockSegmentWriter) Close(ctx context.Context) error {
	if m.closed {
		return nil
	}
	m.closed = true
	return nil
}

func (m *MockSegmentWriter) Count() int {
	return len(m.records)
}

func (m *MockSegmentWriter) Bytes() int64 {
	return m.segmentRef.Bytes
}

func (m *MockSegmentWriter) IsClosed() bool {
	return m.closed
}

func TestSpillConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    SpillConfig
		wantError bool
	}{
		{
			name: "disabled spill config is valid",
			config: SpillConfig{
				SpillEnabled: false,
			},
			wantError: false,
		},
		{
			name: "valid enabled spill config",
			config: SpillConfig{
				Backend:      GCS,
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
				SpillEnabled: true,
			},
			wantError: false,
		},
		{
			name: "missing bucket",
			config: SpillConfig{
				Backend:      GCS,
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
				SpillEnabled: true,
			},
			wantError: true,
		},
		{
			name: "invalid segment size",
			config: SpillConfig{
				Backend:      GCS,
				Bucket:       "test-bucket",
				SegmentBytes: 0,
				ZstdLevel:    6,
				SpillEnabled: true,
			},
			wantError: true,
		},
		{
			name: "invalid zstd level",
			config: SpillConfig{
				Backend:      GCS,
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    25, // too high
				SpillEnabled: true,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewSegmentManager(t *testing.T) {
	tests := []struct {
		name      string
		config    SpillConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: SpillConfig{
				Backend:      GCS,
				Bucket:       "test-bucket",
				SegmentBytes: 8 << 20,
				ZstdLevel:    6,
				SpillEnabled: true,
			},
			wantError: false,
		},
		{
			name: "invalid config",
			config: SpillConfig{
				Backend:      GCS,
				SegmentBytes: 0, // invalid
				ZstdLevel:    6,
				SpillEnabled: true,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &MockWriterSegmentFactory{}
			sm, err := NewSegmentManager(tt.config, factory)
			
			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, sm)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, sm)
			}
		})
	}
}

func TestSegmentManager_SpillTrace(t *testing.T) {
	config := SpillConfig{
		Backend:      GCS,
		Bucket:       "test-bucket",
		Prefix:       "spill-data",
		Codec:        Avro,
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		SpillEnabled: true,
	}

	factory := &MockWriterSegmentFactory{}
	sm, err := NewSegmentManager(config, factory)
	require.NoError(t, err)

	ctx := context.Background()
	traceID := TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	tenant := Tenant("test-tenant")
	spans := &MockEncodedSpans{
		otlpBytes:   []byte("mock-otlp-data"),
		nativeBytes: []byte("mock-native-data"),
	}

	spillRef, err := sm.SpillTrace(ctx, traceID, tenant, spans)
	require.NoError(t, err)

	// Verify the spill reference
	assert.NotEmpty(t, spillRef.URL)
	assert.Equal(t, SpillCodecAvro, spillRef.Codec)
	assert.Equal(t, 1, spillRef.Count)
	assert.Greater(t, spillRef.Bytes, int64(0))
	assert.WithinDuration(t, time.Now(), spillRef.CreatedAt, time.Minute)
}

func TestSegmentManager_SpillTrace_Disabled(t *testing.T) {
	config := SpillConfig{
		SpillEnabled: false,
	}

	factory := &MockWriterSegmentFactory{}
	sm, err := NewSegmentManager(config, factory)
	require.NoError(t, err)

	ctx := context.Background()
	traceID := TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	tenant := Tenant("test-tenant")
	spans := &MockEncodedSpans{
		otlpBytes: []byte("mock-otlp-data"),
	}

	_, err = sm.SpillTrace(ctx, traceID, tenant, spans)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "spill is disabled")
}

func TestSegmentManager_GetSpillConfig(t *testing.T) {
	config := SpillConfig{
		Backend:      S3,
		Bucket:       "test-bucket",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		SpillEnabled: true,
	}

	factory := &MockWriterSegmentFactory{}
	sm, err := NewSegmentManager(config, factory)
	require.NoError(t, err)

	returnedConfig := sm.GetSpillConfig()
	assert.Equal(t, config, returnedConfig)
}

func TestSegmentManager_Close(t *testing.T) {
	config := SpillConfig{
		Backend:      GCS,
		Bucket:       "test-bucket",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
		SpillEnabled: true,
	}

	factory := &MockWriterSegmentFactory{}
	sm, err := NewSegmentManager(config, factory)
	require.NoError(t, err)

	ctx := context.Background()
	err = sm.Close(ctx)
	assert.NoError(t, err)
}

func TestTraceID_Hex(t *testing.T) {
	traceID := TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	hex := traceID.Hex()
	expected := "0102030405060708090a0b0c0d0e0f10"
	assert.Equal(t, expected, hex)
}

func TestConvertCodec(t *testing.T) {
	tests := []struct {
		input    Codec
		expected SpillCodec
	}{
		{Avro, SpillCodecAvro},
		{JSONLZstd, SpillCodecJSONLZstd},
		{Codec(99), SpillCodecAvro}, // unknown codec defaults to Avro
	}

	for _, test := range tests {
		result := convertCodec(test.input)
		assert.Equal(t, test.expected, result)
	}
}