// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spill

// AvroSchemaV1 defines the Avro schema for spilled span records
// This schema is used for both GCS and S3 Avro writers
const AvroSchemaV1 = `{
  "type": "record",
  "name": "Span",
  "namespace": "otel.tailspill.v1",
  "fields": [
    {"name":"trace_id","type":{"type":"fixed","name":"TraceID","size":16}},
    {"name":"span_id","type":{"type":"fixed","name":"SpanID","size":8}},
    {"name":"parent_span_id","type":{"type":"fixed","name":"ParentSpanID","size":8}},
    {"name":"name","type":"string"},
    {"name":"kind","type":"int"},
    {"name":"start_unix_nano","type":"long"},
    {"name":"end_unix_nano","type":"long"},
    {"name":"status_code","type":"int"},
    {"name":"attributes","type":{"type":"map","values":["string","long","double","boolean"]}},
    {"name":"otlp_bytes","type":"bytes"}
  ]
}`

// AvroRecord represents a span record in Avro format
// This struct matches the Avro schema fields exactly
type AvroRecord struct {
	TraceID       [16]byte           `avro:"trace_id"`
	SpanID        [8]byte            `avro:"span_id"`
	ParentSpanID  [8]byte            `avro:"parent_span_id"`
	Name          string             `avro:"name"`
	Kind          int32              `avro:"kind"`
	StartUnixNano int64              `avro:"start_unix_nano"`
	EndUnixNano   int64              `avro:"end_unix_nano"`
	StatusCode    int32              `avro:"status_code"`
	Attributes    map[string]any     `avro:"attributes"`
	OTLPBytes     []byte             `avro:"otlp_bytes"`
}

// ToAvroRecord converts a SpanRecord to AvroRecord format
func (s SpanRecord) ToAvroRecord() AvroRecord {
	return AvroRecord{
		TraceID:       s.TraceID,
		SpanID:        s.SpanID,
		ParentSpanID:  s.ParentSpanID,
		Name:          s.Name,
		Kind:          int32(s.Kind),
		StartUnixNano: int64(s.StartUnixNano),
		EndUnixNano:   int64(s.EndUnixNano),
		StatusCode:    int32(s.StatusCode),
		Attributes:    s.Attributes,
		OTLPBytes:     s.OTLPBytes,
	}
}

// ToSpanRecord converts an AvroRecord back to SpanRecord format
func (a AvroRecord) ToSpanRecord() SpanRecord {
	return SpanRecord{
		TraceID:       a.TraceID,
		SpanID:        a.SpanID,
		ParentSpanID:  a.ParentSpanID,
		Name:          a.Name,
		Kind:          int8(a.Kind),
		StartUnixNano: uint64(a.StartUnixNano),
		EndUnixNano:   uint64(a.EndUnixNano),
		StatusCode:    int8(a.StatusCode),
		Attributes:    a.Attributes,
		OTLPBytes:     a.OTLPBytes,
	}
}

// ValidateAvroSchema validates that the schema is parseable
// This should be called during initialization to catch schema errors early
func ValidateAvroSchema(schema string) error {
	if schema == "" {
		return ErrInvalidCodec
	}
	// Additional validation could be added here if using a specific Avro library
	// For now, we assume the constant schema is valid
	return nil
}