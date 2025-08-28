// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

func TestDefaultSegmentFactory_SupportsBackend(t *testing.T) {
	factory := NewSegmentFactory()

	tests := []struct {
		backend  spill.Backend
		expected bool
	}{
		{spill.GCS, true},
		{spill.S3, true},
		{spill.Backend(99), false},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.backend)), func(t *testing.T) {
			assert.Equal(t, tt.expected, factory.SupportsBackend(tt.backend))
		})
	}
}

func TestDefaultSegmentFactory_SupportsCodec(t *testing.T) {
	factory := NewSegmentFactory()

	tests := []struct {
		codec    spill.Codec
		expected bool
	}{
		{spill.Avro, true},
		{spill.JSONLZstd, true},
		{spill.Codec(99), false},
	}

	for _, tt := range tests {
		t.Run(tt.codec.String(), func(t *testing.T) {
			assert.Equal(t, tt.expected, factory.SupportsCodec(tt.codec))
		})
	}
}

func TestDefaultSegmentFactory_NewWriter_UnsupportedBackend(t *testing.T) {
	factory := NewSegmentFactory()
	ctx := context.Background()

	opts := spill.WriterOpts{
		Backend:      spill.Backend(99),
		Bucket:       "test-bucket",
		TraceIDHex:   "0102030405060708090a0b0c0d0e0f10",
		SegmentBytes: 8 << 20,
		ZstdLevel:    6,
	}

	writer, err := factory.NewWriter(ctx, opts)
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "unsupported backend")
}

func TestDefaultSegmentFactory_NewReader_UnsupportedBackend(t *testing.T) {
	factory := NewSegmentFactory()
	ctx := context.Background()

	reader, err := factory.NewReader(ctx, spill.Backend(99))
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "unsupported backend")
}

func TestDefaultSegmentFactory_ClientFactoryRegistration(t *testing.T) {
	factory := &DefaultSegmentFactory{
		clientFactories: make(map[spill.Backend]ClientFactory),
	}

	// Mock client factory
	mockFactory := &mockClientFactory{}

	// Register factory
	factory.RegisterClientFactory(spill.GCS, mockFactory)

	// Retrieve factory
	retrieved, exists := factory.GetClientFactory(spill.GCS)
	assert.True(t, exists)
	assert.Equal(t, mockFactory, retrieved)

	// Try to retrieve non-existent factory
	_, exists = factory.GetClientFactory(spill.S3)
	assert.False(t, exists)
}

// Mock client factory for testing
type mockClientFactory struct{}

func (m *mockClientFactory) CreateGCSClient(ctx context.Context) (interface{}, error) {
	return "mock-gcs-client", nil
}

func (m *mockClientFactory) CreateS3Client(ctx context.Context) (interface{}, error) {
	return "mock-s3-client", nil
}