// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import "time"

// Config holds the minimal configuration needed by store implementations.
// This is kept separate from the main processor config to avoid cross-pollution.
type Config struct {
	HotLimits HotLimits
}

// HotLimits defines per-trace limits for hot storage.
type HotLimits struct {
	MaxSpanPerTrace  int
	MaxBytesPerTrace int64
	DefaultTTL       time.Duration
	LeaseTTL         time.Duration
}
