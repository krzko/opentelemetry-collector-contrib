// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemory

import (
	"context"
	"io"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store/adapter"
)

// legacyDecodedSpans implements store.DecodedSpans for ptrace.Traces
type legacyDecodedSpans struct {
	traces  ptrace.Traces
	spans   []store.SpanRecord // Cached span records
	adapter *adapter.PtraceAdapter
}

// Len returns the total number of spans
func (d *legacyDecodedSpans) Len() int {
	return d.traces.SpanCount()
}

// At returns a span record at the given index
func (d *legacyDecodedSpans) At(i int) store.SpanRecord {
	// Build span records cache if not already built
	if d.spans == nil {
		d.buildSpanRecords()
	}
	
	if i < 0 || i >= len(d.spans) {
		return store.SpanRecord{} // Return empty record for out of bounds
	}
	
	return d.spans[i]
}

// buildSpanRecords converts ptrace.Traces to []store.SpanRecord using existing adapter
func (d *legacyDecodedSpans) buildSpanRecords() {
	// Use existing adapter to encode traces to EncodedSpans, then decode to records
	encoded, err := d.adapter.EncodePtraceToSpans(d.traces)
	if err != nil {
		d.spans = []store.SpanRecord{} // Return empty on error
		return
	}
	
	records, err := d.adapter.DecodeSpansToRecords(encoded)
	if err != nil {
		d.spans = []store.SpanRecord{} // Return empty on error
		return
	}
	
	d.spans = records
}


// legacySpanIterator implements store.SpanIterator for legacy store
type legacySpanIterator struct {
	spans    store.DecodedSpans
	returned bool
}

// Next returns the next batch of spans
func (it *legacySpanIterator) Next(ctx context.Context) (store.DecodedSpans, error) {
	if it.returned {
		return nil, io.EOF
	}
	
	it.returned = true
	return it.spans, nil
}

// Close releases iterator resources
func (it *legacySpanIterator) Close() error {
	return nil
}