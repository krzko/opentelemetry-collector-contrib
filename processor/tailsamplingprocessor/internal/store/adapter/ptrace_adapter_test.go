// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
)

func TestPtraceAdapter_EncodeDecode(t *testing.T) {
	adapter := NewPtraceAdapter()

	// Create test traces
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	// Add first span
	span1 := ss.Spans().AppendEmpty()
	span1.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span1.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span1.SetParentSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 0})
	span1.SetName("test-span-1")
	span1.SetKind(ptrace.SpanKindServer)
	span1.SetStartTimestamp(1000000)
	span1.SetEndTimestamp(2000000)
	span1.Status().SetCode(ptrace.StatusCodeOk)
	span1.Attributes().PutStr("service.name", "test-service")
	span1.Attributes().PutInt("http.status_code", 200)
	span1.Attributes().PutBool("success", true)

	// Add second span
	span2 := ss.Spans().AppendEmpty()
	span2.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span2.SetSpanID([8]byte{9, 10, 11, 12, 13, 14, 15, 16})
	span2.SetParentSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span2.SetName("test-span-2")
	span2.SetKind(ptrace.SpanKindClient)
	span2.SetStartTimestamp(1500000)
	span2.SetEndTimestamp(1800000)
	span2.Status().SetCode(ptrace.StatusCodeError)

	// Test encoding
	encoded, err := adapter.EncodePtraceToSpans(traces)
	require.NoError(t, err)
	assert.Equal(t, 2, encoded.Count)
	assert.NotNil(t, encoded.OTLP)
	assert.Greater(t, encoded.Bytes, 0)

	// Test decoding
	records, err := adapter.DecodeSpansToRecords(encoded)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	// Verify first span
	record1 := records[0]
	expectedTraceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	assert.Equal(t, expectedTraceID, record1.TraceID)
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}), record1.SpanID)
	assert.Equal(t, "test-span-1", record1.Name)
	assert.Equal(t, int8(ptrace.SpanKindServer), record1.Kind)
	assert.Equal(t, uint64(1000000), record1.StartUnixNano)
	assert.Equal(t, uint64(2000000), record1.EndUnixNano)
	assert.Equal(t, int8(ptrace.StatusCodeOk), record1.StatusCode)

	// Verify attributes
	assert.Equal(t, "test-service", record1.Attributes["service.name"])
	assert.Equal(t, int64(200), record1.Attributes["http.status_code"])
	assert.Equal(t, true, record1.Attributes["success"])

	// Verify second span
	record2 := records[1]
	assert.Equal(t, expectedTraceID, record2.TraceID)
	assert.Equal(t, pcommon.SpanID([8]byte{9, 10, 11, 12, 13, 14, 15, 16}), record2.SpanID)
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}), record2.ParentSpanID)
	assert.Equal(t, "test-span-2", record2.Name)
	assert.Equal(t, int8(ptrace.SpanKindClient), record2.Kind)
	assert.Equal(t, int8(ptrace.StatusCodeError), record2.StatusCode)
}

func TestPtraceAdapter_ConvertRecordsToSpans(t *testing.T) {
	adapter := NewPtraceAdapter()

	// Create test records
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	records := []store.SpanRecord{
		{
			TraceID:       traceID,
			SpanID:        pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}),
			ParentSpanID:  pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 0}),
			Name:          "root-span",
			Kind:          int8(ptrace.SpanKindServer),
			StartUnixNano: 1000000,
			EndUnixNano:   2000000,
			StatusCode:    int8(ptrace.StatusCodeOk),
			Attributes: map[string]any{
				"service.name": "test-service",
				"version":      int64(1),
				"debug":        true,
			},
		},
		{
			TraceID:       traceID,
			SpanID:        pcommon.SpanID([8]byte{9, 10, 11, 12, 13, 14, 15, 16}),
			ParentSpanID:  pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}),
			Name:          "child-span",
			Kind:          int8(ptrace.SpanKindClient),
			StartUnixNano: 1500000,
			EndUnixNano:   1800000,
			StatusCode:    int8(ptrace.StatusCodeError),
			Attributes: map[string]any{
				"operation": "db-query",
				"timeout":   30.5,
			},
		},
	}

	// Convert to traces
	traces, err := adapter.ConvertRecordsToSpans(records)
	require.NoError(t, err)

	// Verify structure
	assert.Equal(t, 1, traces.ResourceSpans().Len())
	rs := traces.ResourceSpans().At(0)
	assert.Equal(t, 1, rs.ScopeSpans().Len())
	ss := rs.ScopeSpans().At(0)
	assert.Equal(t, 2, ss.Spans().Len())

	// Verify first span
	span1 := ss.Spans().At(0)
	assert.Equal(t, traceID, span1.TraceID())
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}), span1.SpanID())
	assert.Equal(t, "root-span", span1.Name())
	assert.Equal(t, ptrace.SpanKindServer, span1.Kind())
	assert.Equal(t, pcommon.Timestamp(1000000), span1.StartTimestamp())
	assert.Equal(t, pcommon.Timestamp(2000000), span1.EndTimestamp())
	assert.Equal(t, ptrace.StatusCodeOk, span1.Status().Code())

	// Verify attributes
	attrs1 := span1.Attributes()
	serviceName, exists := attrs1.Get("service.name")
	assert.True(t, exists)
	assert.Equal(t, "test-service", serviceName.Str())

	version, exists := attrs1.Get("version")
	assert.True(t, exists)
	assert.Equal(t, int64(1), version.Int())

	debug, exists := attrs1.Get("debug")
	assert.True(t, exists)
	assert.True(t, debug.Bool())

	// Verify second span
	span2 := ss.Spans().At(1)
	assert.Equal(t, traceID, span2.TraceID())
	assert.Equal(t, pcommon.SpanID([8]byte{9, 10, 11, 12, 13, 14, 15, 16}), span2.SpanID())
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}), span2.ParentSpanID())
	assert.Equal(t, "child-span", span2.Name())
	assert.Equal(t, ptrace.SpanKindClient, span2.Kind())
	assert.Equal(t, ptrace.StatusCodeError, span2.Status().Code())

	// Verify second span attributes
	attrs2 := span2.Attributes()
	operation, exists := attrs2.Get("operation")
	assert.True(t, exists)
	assert.Equal(t, "db-query", operation.Str())

	timeout, exists := attrs2.Get("timeout")
	assert.True(t, exists)
	assert.Equal(t, 30.5, timeout.Double())
}

func TestPtraceAdapter_EmptyTraces(t *testing.T) {
	adapter := NewPtraceAdapter()

	// Test encoding empty traces
	emptyTraces := ptrace.NewTraces()
	_, err := adapter.EncodePtraceToSpans(emptyTraces)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no spans to encode")

	// Test converting empty records
	traces, err := adapter.ConvertRecordsToSpans([]store.SpanRecord{})
	require.NoError(t, err)
	assert.Equal(t, 0, traces.SpanCount())
}
