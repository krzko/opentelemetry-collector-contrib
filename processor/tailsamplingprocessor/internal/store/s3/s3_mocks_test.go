// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// MockS3Client implements S3Client interface for testing
type MockS3Client struct {
	objects       map[string][]byte
	objectMeta    map[string]*S3HeadObjectOutput
	putError      error
	headError     error
	putCallCount  int
	headCallCount int
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects:    make(map[string][]byte),
		objectMeta: make(map[string]*S3HeadObjectOutput),
	}
}

func (m *MockS3Client) PutObject(ctx context.Context, input *S3PutObjectInput) (*S3PutObjectOutput, error) {
	m.putCallCount++
	
	if m.putError != nil {
		return nil, m.putError
	}

	key := fmt.Sprintf("%s/%s", input.Bucket, input.Key)
	
	// Read the body
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	
	m.objects[key] = data
	
	// Store metadata
	etag := fmt.Sprintf(`"mock-etag-%d"`, m.putCallCount)
	m.objectMeta[key] = &S3HeadObjectOutput{
		ContentLength: int64(len(data)),
		ETag:          etag,
	}

	return &S3PutObjectOutput{
		ETag: etag,
	}, nil
}

func (m *MockS3Client) HeadObject(ctx context.Context, input *S3HeadObjectInput) (*S3HeadObjectOutput, error) {
	m.headCallCount++
	
	if m.headError != nil {
		return nil, m.headError
	}

	key := fmt.Sprintf("%s/%s", input.Bucket, input.Key)
	meta, exists := m.objectMeta[key]
	if !exists {
		return nil, fmt.Errorf("object not found")
	}

	return meta, nil
}

func (m *MockS3Client) GetObject(ctx context.Context, input *S3GetObjectInput) (*S3GetObjectOutput, error) {
	key := fmt.Sprintf("%s/%s", input.Bucket, input.Key)
	
	data, exists := m.objects[key]
	if !exists {
		return nil, fmt.Errorf("object not found: %s", key)
	}
	
	contentType := "application/octet-stream"
	etag := "mock-etag"
	contentLength := int64(len(data))
	
	return &S3GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader(string(data))),
		ContentType:   &contentType,
		ETag:          &etag,
		ContentLength: &contentLength,
	}, nil
}

// SetPutError sets an error to be returned by PutObject
func (m *MockS3Client) SetPutError(err error) {
	m.putError = err
}

// SetHeadError sets an error to be returned by HeadObject
func (m *MockS3Client) SetHeadError(err error) {
	m.headError = err
}

// GetObjectData returns the data stored for a given bucket/key
func (m *MockS3Client) GetObjectData(bucket, key string) ([]byte, bool) {
	fullKey := fmt.Sprintf("%s/%s", bucket, key)
	data, exists := m.objects[fullKey]
	return data, exists
}

// GetPutCallCount returns the number of times PutObject was called
func (m *MockS3Client) GetPutCallCount() int {
	return m.putCallCount
}

// GetHeadCallCount returns the number of times HeadObject was called
func (m *MockS3Client) GetHeadCallCount() int {
	return m.headCallCount
}

// HasObject checks if an object exists in the mock storage
func (m *MockS3Client) HasObject(bucket, key string) bool {
	fullKey := fmt.Sprintf("%s/%s", bucket, key)
	_, exists := m.objects[fullKey]
	return exists
}

// ListObjects returns all object keys with a given prefix
func (m *MockS3Client) ListObjects(bucket, prefix string) []string {
	var keys []string
	searchPrefix := fmt.Sprintf("%s/%s", bucket, prefix)
	
	for key := range m.objects {
		if strings.HasPrefix(key, searchPrefix) {
			// Extract just the object key part (after bucket/)
			parts := strings.SplitN(key, "/", 2)
			if len(parts) > 1 {
				keys = append(keys, parts[1])
			}
		}
	}
	
	return keys
}

// Reset clears all stored objects and resets counters
func (m *MockS3Client) Reset() {
	m.objects = make(map[string][]byte)
	m.objectMeta = make(map[string]*S3HeadObjectOutput)
	m.putCallCount = 0
	m.headCallCount = 0
	m.putError = nil
	m.headError = nil
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