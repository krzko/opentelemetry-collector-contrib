// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultURLBuilder_BuildURL(t *testing.T) {
	builder := NewURLBuilder()

	tests := []struct {
		name     string
		opts     WriterOpts
		contains []string // Substrings that should be present in the URL
	}{
		{
			name: "GCS URL with prefix",
			opts: WriterOpts{
				Backend:    GCS,
				Bucket:     "test-bucket",
				Prefix:     "spill-data",
				Tenant:     "test-tenant",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				Seq:        123,
				Codec:      Avro,
			},
			contains: []string{
				"gs://test-bucket/",
				"spill-data/",
				"tenant=test-tenant/",
				"trace=0102030405060708090a0b0c0d0e0f10/",
				"part-123.avro",
			},
		},
		{
			name: "S3 URL without prefix",
			opts: WriterOpts{
				Backend:    S3,
				Bucket:     "my-s3-bucket",
				Tenant:     "prod-tenant",
				TraceIDHex: "abcdef1234567890abcdef1234567890",
				Seq:        456,
				Codec:      JSONLZstd,
			},
			contains: []string{
				"s3://my-s3-bucket/",
				"tenant=prod-tenant/",
				"trace=abcdef1234567890abcdef1234567890/",
				"part-456.jsonl.zst",
			},
		},
		{
			name: "GCS URL with empty prefix",
			opts: WriterOpts{
				Backend:    GCS,
				Bucket:     "another-bucket",
				Prefix:     "",
				Tenant:     "empty-prefix-tenant",
				TraceIDHex: "1111222233334444555566667777888",
				Seq:        0,
				Codec:      Avro,
			},
			contains: []string{
				"gs://another-bucket/",
				"tenant=empty-prefix-tenant/",
				"trace=1111222233334444555566667777888",
				"part-0.avro",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := builder.BuildURL(tt.opts)
			t.Logf("Generated URL: %s", url)

			for _, substring := range tt.contains {
				assert.Contains(t, url, substring, "URL should contain: %s", substring)
			}

			// Check that URL contains current date
			today := time.Now().UTC().Format("2006-01-02")
			assert.Contains(t, url, "date="+today)
		})
	}
}

func TestDefaultURLBuilder_ParseURL(t *testing.T) {
	builder := NewURLBuilder()

	tests := []struct {
		name     string
		url      string
		expected WriterOpts
		wantErr  bool
	}{
		{
			name: "valid GCS URL with prefix",
			url:  "gs://test-bucket/spill-data/tenant=test-tenant/date=2024-01-15/trace=0102030405060708090a0b0c0d0e0f10/part-123.avro",
			expected: WriterOpts{
				Backend:    GCS,
				Bucket:     "test-bucket",
				Prefix:     "spill-data",
				Tenant:     "test-tenant",
				TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
				Seq:        123,
				Codec:      Avro,
			},
			wantErr: false,
		},
		{
			name: "valid S3 URL without prefix",
			url:  "s3://my-bucket/tenant=prod/date=2024-02-20/trace=abcdef1234567890abcdef1234567890/part-456.jsonl.zst",
			expected: WriterOpts{
				Backend:    S3,
				Bucket:     "my-bucket",
				Prefix:     "",
				Tenant:     "prod",
				TraceIDHex: "abcdef1234567890abcdef1234567890",
				Seq:        456,
				Codec:      JSONLZstd,
			},
			wantErr: false,
		},
		{
			name: "valid GCS URL with nested prefix",
			url:  "gs://bucket/prefix/sub/tenant=test/date=2024-03-10/trace=1234567890abcdef1234567890abcdef/part-0.avro",
			expected: WriterOpts{
				Backend:    GCS,
				Bucket:     "bucket",
				Prefix:     "prefix/sub",
				Tenant:     "test",
				TraceIDHex: "1234567890abcdef1234567890abcdef",
				Seq:        0,
				Codec:      Avro,
			},
			wantErr: false,
		},
		{
			name:    "invalid URL format",
			url:     "not-a-valid-url",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			url:     "http://example.com/path",
			wantErr: true,
		},
		{
			name:    "missing bucket",
			url:     "gs:///tenant=test/date=2024-01-01/trace=abcd/part-0.avro",
			wantErr: true,
		},
		{
			name:    "invalid path format",
			url:     "gs://bucket/invalid/path/structure",
			wantErr: true,
		},
		{
			name:    "invalid date format",
			url:     "gs://bucket/tenant=test/date=invalid-date/trace=abcd/part-0.avro",
			wantErr: true,
		},
		{
			name:    "invalid sequence number",
			url:     "gs://bucket/tenant=test/date=2024-01-01/trace=abcd/part-invalid.avro",
			wantErr: true,
		},
		{
			name:    "unsupported codec",
			url:     "gs://bucket/tenant=test/date=2024-01-01/trace=abcd/part-0.unknown",
			wantErr: true,
		},
		{
			name:    "invalid trace ID length",
			url:     "gs://bucket/tenant=test/date=2024-01-01/trace=short/part-0.avro",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := builder.ParseURL(tt.url)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected.Backend, opts.Backend)
			assert.Equal(t, tt.expected.Bucket, opts.Bucket)
			assert.Equal(t, tt.expected.Prefix, opts.Prefix)
			assert.Equal(t, tt.expected.Tenant, opts.Tenant)
			assert.Equal(t, tt.expected.TraceIDHex, opts.TraceIDHex)
			assert.Equal(t, tt.expected.Seq, opts.Seq)
			assert.Equal(t, tt.expected.Codec, opts.Codec)
		})
	}
}

func TestDefaultURLBuilder_RoundTrip(t *testing.T) {
	builder := NewURLBuilder()

	// Create test options
	originalOpts := WriterOpts{
		Backend:    GCS,
		Bucket:     "test-bucket",
		Prefix:     "spill/data",
		Tenant:     "test-tenant",
		TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
		Seq:        789,
		Codec:      JSONLZstd,
	}

	// Build URL
	url := builder.BuildURL(originalOpts)
	t.Logf("Generated URL: %s", url)

	// Parse URL back
	parsedOpts, err := builder.ParseURL(url)
	require.NoError(t, err)

	// Compare (note: we can't compare dates exactly due to time progression)
	assert.Equal(t, originalOpts.Backend, parsedOpts.Backend)
	assert.Equal(t, originalOpts.Bucket, parsedOpts.Bucket)
	assert.Equal(t, originalOpts.Prefix, parsedOpts.Prefix)
	assert.Equal(t, originalOpts.Tenant, parsedOpts.Tenant)
	assert.Equal(t, originalOpts.TraceIDHex, parsedOpts.TraceIDHex)
	assert.Equal(t, originalOpts.Seq, parsedOpts.Seq)
	assert.Equal(t, originalOpts.Codec, parsedOpts.Codec)
}

func TestGenerateFileExtension(t *testing.T) {
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
			assert.Equal(t, tt.expected, GenerateFileExtension(tt.codec))
		})
	}
}

func TestValidateURLComponents(t *testing.T) {
	tests := []struct {
		name       string
		bucket     string
		tenant     string
		traceIDHex string
		wantErr    bool
	}{
		{
			name:       "valid components",
			bucket:     "test-bucket",
			tenant:     "test-tenant",
			traceIDHex: "0102030405060708090a0b0c0d0e0f10",
			wantErr:    false,
		},
		{
			name:       "empty bucket",
			bucket:     "",
			tenant:     "test-tenant",
			traceIDHex: "0102030405060708090a0b0c0d0e0f10",
			wantErr:    true,
		},
		{
			name:       "empty tenant",
			bucket:     "test-bucket",
			tenant:     "",
			traceIDHex: "0102030405060708090a0b0c0d0e0f10",
			wantErr:    true,
		},
		{
			name:       "short trace ID",
			bucket:     "test-bucket",
			tenant:     "test-tenant",
			traceIDHex: "short",
			wantErr:    true,
		},
		{
			name:       "long trace ID",
			bucket:     "test-bucket",
			tenant:     "test-tenant",
			traceIDHex: "0102030405060708090a0b0c0d0e0f10extra",
			wantErr:    true,
		},
		{
			name:       "invalid hex characters",
			bucket:     "test-bucket",
			tenant:     "test-tenant",
			traceIDHex: "0102030405060708090a0b0c0d0e0fXX",
			wantErr:    true,
		},
		{
			name:       "uppercase hex is valid",
			bucket:     "test-bucket",
			tenant:     "test-tenant",
			traceIDHex: "0102030405060708090A0B0C0D0E0F10",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURLComponents(tt.bucket, tt.tenant, tt.traceIDHex)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDefaultURLBuilder_EdgeCases(t *testing.T) {
	builder := NewURLBuilder()

	t.Run("prefix with leading/trailing slashes", func(t *testing.T) {
		opts := WriterOpts{
			Backend:    GCS,
			Bucket:     "test-bucket",
			Prefix:     "/prefix/with/slashes/",
			Tenant:     "test-tenant",
			TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
			Seq:        1,
			Codec:      Avro,
		}

		url := builder.BuildURL(opts)
		
		// Should not have double slashes after the scheme
		pathPart := url[5:] // Remove "gs://" part
		assert.NotContains(t, pathPart, "//", "URL path should not contain double slashes")
		assert.Contains(t, url, "prefix/with/slashes/")
	})

	t.Run("unknown backend", func(t *testing.T) {
		opts := WriterOpts{
			Backend:    Backend(99),
			Bucket:     "test-bucket",
			Tenant:     "test-tenant",
			TraceIDHex: "0102030405060708090a0b0c0d0e0f10",
			Seq:        1,
			Codec:      Avro,
		}

		url := builder.BuildURL(opts)
		assert.Contains(t, url, "unknown://")
	})
}