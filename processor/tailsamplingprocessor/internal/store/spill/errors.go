// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

import "errors"

var (
	// Configuration errors
	ErrInvalidTraceID     = errors.New("spill: invalid trace ID")
	ErrInvalidBucket      = errors.New("spill: invalid bucket name")
	ErrInvalidSegmentSize = errors.New("spill: invalid segment size")
	ErrInvalidZstdLevel   = errors.New("spill: invalid zstd compression level")
	ErrInvalidCodec       = errors.New("spill: invalid codec")
	ErrInvalidBackend     = errors.New("spill: invalid backend")
	ErrInvalidURL         = errors.New("spill: invalid URL")
	ErrUnsupportedCodec   = errors.New("spill: unsupported codec")

	// Writer errors
	ErrWriterClosed    = errors.New("spill: writer is closed")
	ErrSegmentFull     = errors.New("spill: segment is full")
	ErrFlushFailed     = errors.New("spill: flush operation failed")
	ErrEncodingFailed  = errors.New("spill: encoding failed")
	ErrCompressionFailed = errors.New("spill: compression failed")

	// Storage errors
	ErrStorageUnavailable = errors.New("spill: storage backend unavailable")
	ErrUploadFailed      = errors.New("spill: upload failed")
	ErrIntegrityCheckFailed = errors.New("spill: integrity check failed")

	// Reader errors
	ErrSegmentNotFound      = errors.New("spill: segment not found")
	ErrDecodingFailed       = errors.New("spill: decoding failed")
	ErrDecompressionFailed  = errors.New("spill: decompression failed")
	ErrCorruptedData        = errors.New("spill: corrupted data")
	ErrReaderNotImplemented = errors.New("spill: reader not implemented")
)