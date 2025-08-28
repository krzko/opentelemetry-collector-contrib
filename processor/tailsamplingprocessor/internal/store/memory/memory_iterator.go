// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"io"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// memDecodedSpans implements store.DecodedSpans for in-memory spans.
type memDecodedSpans struct {
	spans []store.SpanRecord
}

func (d *memDecodedSpans) Len() int {
	return len(d.spans)
}

func (d *memDecodedSpans) At(i int) store.SpanRecord {
	if i < 0 || i >= len(d.spans) {
		return store.SpanRecord{}
	}
	return d.spans[i]
}

// memSpanIterator implements store.SpanIterator for in-memory spans.
type memSpanIterator struct {
	spans     []store.SpanRecord
	batchSize int
	pos       int
}

func (it *memSpanIterator) Next(ctx context.Context) (store.DecodedSpans, error) {
	if it.pos >= len(it.spans) {
		return nil, io.EOF
	}

	end := it.pos + it.batchSize
	if end > len(it.spans) {
		end = len(it.spans)
	}

	batch := &memDecodedSpans{
		spans: it.spans[it.pos:end],
	}

	it.pos = end
	return batch, nil
}

func (it *memSpanIterator) Close() error {
	// No resources to clean up for memory iterator
	return nil
}
