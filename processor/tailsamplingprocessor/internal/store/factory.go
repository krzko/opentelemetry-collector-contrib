// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/gcs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/s3"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/spill"
)

// DefaultSegmentFactory implements spill.SegmentFactory interface
// It provides a centralized way to create writers and readers for different backends
type DefaultSegmentFactory struct {
	// clientFactories stores backend-specific client factories
	// This allows for dependency injection and testing
	clientFactories map[spill.Backend]ClientFactory
}

// ClientFactory abstracts the creation of cloud storage clients
// This interface allows for easy mocking and testing
type ClientFactory interface {
	// CreateGCSClient creates a new GCS client
	CreateGCSClient(ctx context.Context) (interface{}, error)
	
	// CreateS3Client creates a new S3 client  
	CreateS3Client(ctx context.Context) (interface{}, error)
}

// NewSegmentFactory creates a new segment factory with default client factories
func NewSegmentFactory() spill.SegmentFactory {
	return &DefaultSegmentFactory{
		clientFactories: make(map[spill.Backend]ClientFactory),
	}
}

// NewSegmentFactoryWithClients creates a factory with custom client factories
// This is useful for testing or when specific client configurations are needed
func NewSegmentFactoryWithClients(clientFactories map[spill.Backend]ClientFactory) spill.SegmentFactory {
	return &DefaultSegmentFactory{
		clientFactories: clientFactories,
	}
}

// NewWriter creates a new segment writer based on the configuration
func (f *DefaultSegmentFactory) NewWriter(ctx context.Context, opts spill.WriterOpts) (spill.SegmentWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid writer options: %w", err)
	}

	switch opts.Backend {
	case spill.GCS:
		return f.createGCSWriter(ctx, opts)
	case spill.S3:
		return f.createS3Writer(ctx, opts)
	default:
		return nil, fmt.Errorf("unsupported backend: %v", opts.Backend)
	}
}

// NewReader creates a new segment reader for the specified backend
func (f *DefaultSegmentFactory) NewReader(ctx context.Context, backend spill.Backend) (spill.SegmentReader, error) {
	switch backend {
	case spill.GCS:
		return f.createGCSReader(ctx)
	case spill.S3:
		return f.createS3Reader(ctx)
	default:
		return nil, fmt.Errorf("unsupported backend: %v", backend)
	}
}

// SupportsBackend returns true if the factory supports the given backend
func (f *DefaultSegmentFactory) SupportsBackend(backend spill.Backend) bool {
	switch backend {
	case spill.GCS, spill.S3:
		return true
	default:
		return false
	}
}

// SupportsCodec returns true if the factory supports the given codec
func (f *DefaultSegmentFactory) SupportsCodec(codec spill.Codec) bool {
	switch codec {
	case spill.Avro, spill.JSONLZstd:
		return true
	default:
		return false
	}
}

// createGCSWriter creates a GCS-specific writer
func (f *DefaultSegmentFactory) createGCSWriter(ctx context.Context, opts spill.WriterOpts) (spill.SegmentWriter, error) {
	// Get GCS client factory
	clientFactory, exists := f.GetClientFactory(spill.GCS)
	if !exists {
		return nil, fmt.Errorf("GCS client factory not registered")
	}

	// Create GCS client
	client, err := clientFactory.CreateGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Create GCS writer with the client
	return gcs.NewGCSWriterWithClient(ctx, client, opts)
}

// createS3Writer creates an S3-specific writer
func (f *DefaultSegmentFactory) createS3Writer(ctx context.Context, opts spill.WriterOpts) (spill.SegmentWriter, error) {
	// Get S3 client factory
	clientFactory, exists := f.GetClientFactory(spill.S3)
	if !exists {
		return nil, fmt.Errorf("S3 client factory not registered")
	}

	// Create S3 client
	client, err := clientFactory.CreateS3Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create S3 writer with the client
	return s3.NewS3WriterWithClient(ctx, client, opts)
}

// createGCSReader creates a GCS-specific reader
func (f *DefaultSegmentFactory) createGCSReader(ctx context.Context) (spill.SegmentReader, error) {
	// Get GCS client factory
	clientFactory, exists := f.GetClientFactory(spill.GCS)
	if !exists {
		return nil, fmt.Errorf("GCS client factory not registered")
	}

	// Create GCS client
	client, err := clientFactory.CreateGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Type assert the client to GCS client interface
	gcsClient, ok := client.(gcs.GCSClient)
	if !ok {
		return nil, fmt.Errorf("invalid GCS client type")
	}

	// Create GCS reader with the client
	return gcs.NewGCSReader(gcsClient)
}

// createS3Reader creates an S3-specific reader
func (f *DefaultSegmentFactory) createS3Reader(ctx context.Context) (spill.SegmentReader, error) {
	// Get S3 client factory
	clientFactory, exists := f.GetClientFactory(spill.S3)
	if !exists {
		return nil, fmt.Errorf("S3 client factory not registered")
	}

	// Create S3 client
	client, err := clientFactory.CreateS3Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Type assert the client to S3 client interface
	s3Client, ok := client.(s3.S3Client)
	if !ok {
		return nil, fmt.Errorf("invalid S3 client type")
	}

	// Create S3 reader with the client
	return s3.NewS3Reader(s3Client)
}

// RegisterClientFactory allows registering custom client factories for different backends
func (f *DefaultSegmentFactory) RegisterClientFactory(backend spill.Backend, factory ClientFactory) {
	f.clientFactories[backend] = factory
}

// GetClientFactory retrieves the client factory for a specific backend
func (f *DefaultSegmentFactory) GetClientFactory(backend spill.Backend) (ClientFactory, bool) {
	factory, exists := f.clientFactories[backend]
	return factory, exists
}

