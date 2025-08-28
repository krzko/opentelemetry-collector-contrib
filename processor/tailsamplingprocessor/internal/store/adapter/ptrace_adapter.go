// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

// PtraceAdapter converts between ptrace types and store types.
type PtraceAdapter struct{}

// NewPtraceAdapter creates a new adapter for ptrace conversions.
func NewPtraceAdapter() *PtraceAdapter {
	return &PtraceAdapter{}
}

// EncodePtraceToSpans converts ptrace.Traces to store.EncodedSpans.
func (a *PtraceAdapter) EncodePtraceToSpans(traces ptrace.Traces) (store.EncodedSpans, error) {
	if traces.SpanCount() == 0 {
		return store.EncodedSpans{}, errors.New("no spans to encode")
	}

	// Marshal to OTLP format
	marshaler := &ptrace.ProtoMarshaler{}
	otlpBytes, err := marshaler.MarshalTraces(traces)
	if err != nil {
		return store.EncodedSpans{}, err
	}

	return store.EncodedSpans{
		OTLP:  otlpBytes,
		Count: traces.SpanCount(),
		Bytes: len(otlpBytes),
	}, nil
}

// DecodeSpansToRecords converts store.EncodedSpans to []store.SpanRecord.
func (a *PtraceAdapter) DecodeSpansToRecords(encoded store.EncodedSpans) ([]store.SpanRecord, error) {
	if encoded.OTLP == nil {
		return nil, errors.New("no OTLP data to decode")
	}

	// Unmarshal OTLP bytes
	unmarshaler := &ptrace.ProtoUnmarshaler{}
	traces, err := unmarshaler.UnmarshalTraces(encoded.OTLP)
	if err != nil {
		return nil, err
	}

	var records []store.SpanRecord

	// Convert each span to SpanRecord
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				record := a.convertSpanToRecord(span, encoded.OTLP)
				records = append(records, record)
			}
		}
	}

	return records, nil
}

// convertSpanToRecord converts a ptrace.Span to store.SpanRecord.
func (a *PtraceAdapter) convertSpanToRecord(span ptrace.Span, rawOTLP []byte) store.SpanRecord {
	traceID := span.TraceID()
	spanID := span.SpanID()
	parentSpanID := span.ParentSpanID()

	// Convert attributes
	attrs := make(map[string]any)
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			attrs[k] = v.Str()
		case pcommon.ValueTypeBool:
			attrs[k] = v.Bool()
		case pcommon.ValueTypeInt:
			attrs[k] = v.Int()
		case pcommon.ValueTypeDouble:
			attrs[k] = v.Double()
		case pcommon.ValueTypeBytes:
			attrs[k] = v.Bytes().AsRaw()
		default:
			attrs[k] = v.AsString()
		}
		return true
	})

	return store.SpanRecord{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		Name:          span.Name(),
		Kind:          int8(span.Kind()),
		StartUnixNano: uint64(span.StartTimestamp()),
		EndUnixNano:   uint64(span.EndTimestamp()),
		StatusCode:    int8(span.Status().Code()),
		Attributes:    attrs,
		Raw:           rawOTLP, // Store reference to full OTLP bytes
	}
}

// ConvertRecordsToSpans converts []store.SpanRecord back to ptrace.Spans.
func (a *PtraceAdapter) ConvertRecordsToSpans(records []store.SpanRecord) (ptrace.Traces, error) {
	traces := ptrace.NewTraces()

	if len(records) == 0 {
		return traces, nil
	}

	// Group spans by trace ID for proper reconstruction
	traceGroups := make(map[store.TraceID][]store.SpanRecord)
	for _, record := range records {
		traceGroups[record.TraceID] = append(traceGroups[record.TraceID], record)
	}

	for traceID, spanRecords := range traceGroups {
		rs := traces.ResourceSpans().AppendEmpty()
		ss := rs.ScopeSpans().AppendEmpty()

		for _, record := range spanRecords {
			span := ss.Spans().AppendEmpty()
			a.populateSpanFromRecord(span, record, traceID)
		}
	}

	return traces, nil
}

// populateSpanFromRecord populates a ptrace.Span from a store.SpanRecord.
func (a *PtraceAdapter) populateSpanFromRecord(span ptrace.Span, record store.SpanRecord, traceID store.TraceID) {
	span.SetTraceID(traceID)
	span.SetSpanID(record.SpanID)
	span.SetParentSpanID(record.ParentSpanID)
	span.SetName(record.Name)
	span.SetKind(ptrace.SpanKind(record.Kind))
	span.SetStartTimestamp(pcommon.Timestamp(record.StartUnixNano))
	span.SetEndTimestamp(pcommon.Timestamp(record.EndUnixNano))
	span.Status().SetCode(ptrace.StatusCode(record.StatusCode))

	// Set attributes
	attrs := span.Attributes()
	for k, v := range record.Attributes {
		switch val := v.(type) {
		case string:
			attrs.PutStr(k, val)
		case bool:
			attrs.PutBool(k, val)
		case int64:
			attrs.PutInt(k, val)
		case float64:
			attrs.PutDouble(k, val)
		case []byte:
			// Convert bytes to base64 string for storage
			attrs.PutStr(k, string(val))
		default:
			// Fallback to string representation
			attrs.PutStr(k, "unknown")
		}
	}
}
