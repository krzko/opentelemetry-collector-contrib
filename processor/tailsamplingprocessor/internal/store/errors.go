// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import "errors"

var (
	// Control-flow / expected errors:
	ErrNotFound      = errors.New("store: trace not found")
	ErrUseIterator   = errors.New("store: use iterator for streaming fetch")
	ErrLeaseOwned    = errors.New("store: lease already owned by another worker")
	ErrLeaseMismatch = errors.New("store: lease release by non-owner")
	ErrCapacity      = errors.New("store: capacity/backpressure")
	ErrBackendDown   = errors.New("store: downstream/exporter unavailable")
	ErrSpillDown     = errors.New("store: object storage unavailable")
	ErrCodecMismatch = errors.New("store: codec/schema mismatch")
	ErrContext       = errors.New("store: context cancelled/deadline")

	// Non-retryable corruption:
	ErrCorrupt = errors.New("store: data corrupt")
)
