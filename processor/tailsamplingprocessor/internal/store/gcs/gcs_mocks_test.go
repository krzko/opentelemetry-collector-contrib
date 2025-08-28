// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"io"
)

// MockGCSClient implements GCSClient interface for testing
type MockGCSClient struct {
	buckets map[string]*MockGCSBucket
}

func NewMockGCSClient() *MockGCSClient {
	return &MockGCSClient{
		buckets: make(map[string]*MockGCSBucket),
	}
}

func (m *MockGCSClient) Bucket(name string) GCSBucket {
	if bucket, exists := m.buckets[name]; exists {
		return bucket
	}
	bucket := &MockGCSBucket{
		name:    name,
		objects: make(map[string]*MockGCSObject),
	}
	m.buckets[name] = bucket
	return bucket
}

// MockGCSBucket implements GCSBucket interface for testing
type MockGCSBucket struct {
	name    string
	objects map[string]*MockGCSObject
}

func (m *MockGCSBucket) Object(name string) GCSObject {
	if obj, exists := m.objects[name]; exists {
		return obj
	}
	obj := &MockGCSObject{
		name:   name,
		bucket: m,
	}
	m.objects[name] = obj
	return obj
}

// MockGCSObject implements GCSObject interface for testing
type MockGCSObject struct {
	name   string
	bucket *MockGCSBucket
	attrs  *GCSObjectAttrs
	data   []byte
}

func (m *MockGCSObject) NewWriter(_ context.Context) GCSObjectWriter {
	return &MockGCSObjectWriter{
		object: m,
	}
}

func (m *MockGCSObject) NewReader(_ context.Context) io.ReadCloser {
	// Return a reader for the mock data
	return io.NopCloser(bytes.NewReader(m.data))
}

func (m *MockGCSObject) Attrs(_ context.Context) (*GCSObjectAttrs, error) {
	if m.attrs == nil {
		m.attrs = &GCSObjectAttrs{
			Size:   int64(len(m.data)),
			Etag:   "mock-etag",
			CRC32C: 12345,
		}
	}
	return m.attrs, nil
}

// MockGCSObjectWriter implements GCSObjectWriter interface for testing
type MockGCSObjectWriter struct {
	object      *MockGCSObject
	data        []byte
	closed      bool
	kmsKeyName  string
	contentType string
}

func (m *MockGCSObjectWriter) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *MockGCSObjectWriter) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	m.object.data = m.data
	return nil
}

func (m *MockGCSObjectWriter) SetKMSKeyName(keyName string) {
	m.kmsKeyName = keyName
}

func (m *MockGCSObjectWriter) SetContentType(contentType string) {
	m.contentType = contentType
}

// MockEncoder implements Encoder interface for testing
type MockEncoder struct {
	data   []byte
	closed bool
}

func NewMockEncoder() *MockEncoder {
	return &MockEncoder{}
}

func (m *MockEncoder) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *MockEncoder) Close() error {
	m.closed = true
	return nil
}

func (m *MockEncoder) Flush() error {
	return nil
}

// MockAvroWriter implements AvroWriter interface for testing
type MockAvroWriter struct {
	records []map[string]any
	closed  bool
}

func NewMockAvroWriter() *MockAvroWriter {
	return &MockAvroWriter{
		records: make([]map[string]any, 0),
	}
}

func (m *MockAvroWriter) Append(record map[string]any) error {
	if m.closed {
		return io.ErrClosedPipe
	}
	m.records = append(m.records, record)
	return nil
}

func (m *MockAvroWriter) Close() error {
	m.closed = true
	return nil
}

func (m *MockAvroWriter) GetRecords() []map[string]any {
	return m.records
}
