// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DefaultURLBuilder implements URLBuilder interface for standard object storage URLs
type DefaultURLBuilder struct{}

// NewURLBuilder creates a new URL builder instance
func NewURLBuilder() URLBuilder {
	return &DefaultURLBuilder{}
}

// BuildURL creates a complete object URL following the partitioning scheme
// Format: gs://{bucket}/{prefix}/tenant={tenant}/date={yyyy-mm-dd}/trace={traceID}/part-{seq}.{extension}
// Format: s3://{bucket}/{prefix}/tenant={tenant}/date={yyyy-mm-dd}/trace={traceID}/part-{seq}.{extension}
func (b *DefaultURLBuilder) BuildURL(opts WriterOpts) string {
	var scheme string
	switch opts.Backend {
	case GCS:
		scheme = "gs"
	case S3:
		scheme = "s3"
	default:
		scheme = "unknown"
	}

	// Generate current date for partitioning
	date := time.Now().UTC().Format("2006-01-02")
	
	// Build path components
	var pathParts []string
	
	// Add optional prefix
	if opts.Prefix != "" {
		cleanPrefix := strings.Trim(opts.Prefix, "/")
		if cleanPrefix != "" {
			pathParts = append(pathParts, cleanPrefix)
		}
	}
	
	// Add partitioning components
	pathParts = append(pathParts,
		fmt.Sprintf("tenant=%s", opts.Tenant),
		fmt.Sprintf("date=%s", date),
		fmt.Sprintf("trace=%s", opts.TraceIDHex),
		fmt.Sprintf("part-%d.%s", opts.Seq, opts.Codec.String()),
	)
	
	// Join all parts
	path := strings.Join(pathParts, "/")
	
	return fmt.Sprintf("%s://%s/%s", scheme, opts.Bucket, path)
}

// ParseURL extracts components from an object storage URL
func (b *DefaultURLBuilder) ParseURL(urlStr string) (WriterOpts, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return WriterOpts{}, fmt.Errorf("invalid URL format: %w", err)
	}

	var backend Backend
	switch parsedURL.Scheme {
	case "gs":
		backend = GCS
	case "s3":
		backend = S3
	default:
		return WriterOpts{}, fmt.Errorf("unsupported URL scheme: %s", parsedURL.Scheme)
	}

	bucket := parsedURL.Host
	if bucket == "" {
		return WriterOpts{}, fmt.Errorf("missing bucket in URL")
	}

	// Parse path components
	path := strings.Trim(parsedURL.Path, "/")
	if path == "" {
		return WriterOpts{}, fmt.Errorf("empty path in URL")
	}

	// Extract components using regex
	opts, err := b.parseURLPath(path, backend, bucket)
	if err != nil {
		return WriterOpts{}, fmt.Errorf("failed to parse URL path: %w", err)
	}

	return opts, nil
}

// parseURLPath extracts components from the URL path
func (b *DefaultURLBuilder) parseURLPath(path string, backend Backend, bucket string) (WriterOpts, error) {
	// Regex pattern to match the expected URL structure
	// Supports optional prefix before tenant= part
	pattern := `^(?:(.+?)/)?tenant=([^/]+)/date=([^/]+)/trace=([^/]+)/part-(\d+)\.(.+)$`
	re := regexp.MustCompile(pattern)
	
	matches := re.FindStringSubmatch(path)
	if len(matches) != 7 {
		return WriterOpts{}, fmt.Errorf("URL path does not match expected pattern")
	}

	prefix := matches[1] // Can be empty
	tenant := matches[2]
	dateStr := matches[3]
	traceIDHex := matches[4]
	seqStr := matches[5]
	codecStr := matches[6]

	// Validate date format
	_, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return WriterOpts{}, fmt.Errorf("invalid date format: %s", dateStr)
	}

	// Parse sequence number
	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		return WriterOpts{}, fmt.Errorf("invalid sequence number: %s", seqStr)
	}

	// Parse codec
	var codec Codec
	switch codecStr {
	case "avro":
		codec = Avro
	case "jsonl.zst":
		codec = JSONLZstd
	default:
		return WriterOpts{}, fmt.Errorf("unsupported codec: %s", codecStr)
	}

	// Validate trace ID format (should be 32 hex chars)
	if len(traceIDHex) != 32 {
		return WriterOpts{}, fmt.Errorf("invalid trace ID format: %s", traceIDHex)
	}

	return WriterOpts{
		Backend:    backend,
		Bucket:     bucket,
		Prefix:     prefix,
		Tenant:     tenant,
		TraceIDHex: traceIDHex,
		Seq:        seq,
		Codec:      codec,
	}, nil
}

// GenerateFileExtension returns the appropriate file extension for the codec
func GenerateFileExtension(codec Codec) string {
	switch codec {
	case Avro:
		return "avro"
	case JSONLZstd:
		return "jsonl.zst"
	default:
		return "unknown"
	}
}

// ValidateURLComponents validates that URL components are valid
func ValidateURLComponents(bucket, tenant, traceIDHex string) error {
	if bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	}
	if tenant == "" {
		return fmt.Errorf("tenant cannot be empty")
	}
	if len(traceIDHex) != 32 {
		return fmt.Errorf("trace ID must be 32 hex characters")
	}
	
	// Validate hex format
	for _, r := range traceIDHex {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return fmt.Errorf("trace ID must contain only hex characters")
		}
	}
	
	return nil
}