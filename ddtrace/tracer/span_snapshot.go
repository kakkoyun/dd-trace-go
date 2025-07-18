// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"maps"
	"slices"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/internal/locking/assert"
	"github.com/tinylib/msgp/msgp"
)

//go:generate go run github.com/tinylib/msgp -unexported -marshal=false -o=span_snapshot_msgp.go -tests=false

type (
	// serializableTrace implements msgp.Encodable on top of a slice of spans.
	serializableTrace []spanSnapshot

	// serializableTraceList implements msgp.Decodable on top of a slice of spanList.
	// NOTICE: This type is only used in tests.
	serializableTraceList []serializableTrace
)

var (
	_ msgp.Encodable = (*serializableTrace)(nil)
	_ readOnlySpan   = (*spanSnapshot)(nil)
	_ payloadItem    = (*spanSnapshot)(nil)

	// Only used in tests.
	_ msgp.Decodable = (*serializableTraceList)(nil)
)

// spanSnapshot is an immutable copy of a Span used during serialization.
// It purposefully omits the mutex and any fields that do not participate in
// the msgpack payload. Only the fields required for encoding are kept.
//
// NOTE: Do *not* add methods or logic on this type; it is a dumb data‐bag.
type spanSnapshot struct {
	// msgpack tags replicate those of Span so that the encoder can be swapped
	// without further changes when we move the writer to snapshots.
	name        string             `msg:"name"`
	service     string             `msg:"service"`
	resource    string             `msg:"resource"`
	spanType    string             `msg:"type"`
	start       int64              `msg:"start"`
	duration    int64              `msg:"duration"`
	metas       map[string]string  `msg:"meta,omitempty"`
	metaStruct  metaStructMap      `msg:"meta_struct,omitempty"`
	metrics     map[string]float64 `msg:"metrics,omitempty"`
	spanID      uint64             `msg:"span_id"`
	traceID     uint64             `msg:"trace_id"`
	parentID    uint64             `msg:"parent_id"`
	errorStatus int32              `msg:"error"`
	spanLinks   []SpanLink         `msg:"span_links,omitempty"`
	spanEvents  []spanEvent        `msg:"span_events,omitempty"`
}

// snapshot copies the mutable span into an immutable spanSnapshot.
func (s *Span) snapshot() spanSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotWhileLocked()
}

// +checklocks:s.mu
func (s *Span) snapshotWhileLocked() spanSnapshot {
	assert.MutexLocked(&s.mu)

	snap := spanSnapshot{
		name:        s.name,
		service:     s.service,
		resource:    s.resource,
		spanType:    s.spanType,
		start:       s.start,
		duration:    s.duration,
		spanID:      s.spanID,
		traceID:     s.traceID,
		parentID:    s.parentID,
		errorStatus: s.errorStatus,
	}
	if len(s.meta) > 0 {
		snap.metas = maps.Clone(s.meta)
	}
	if len(s.metaStruct) > 0 {
		snap.metaStruct = maps.Clone(s.metaStruct)
	}
	if len(s.metrics) > 0 {
		snap.metrics = maps.Clone(s.metrics)
	}
	if len(s.spanLinks) > 0 {
		snap.spanLinks = slices.Clone(s.spanLinks)
	}
	if len(s.spanEvents) > 0 {
		snap.spanEvents = slices.Clone(s.spanEvents)
	}
	return snap
}

func (s spanSnapshot) isResourcePIISafe() bool {
	return s.spanType == ext.SpanTypeWeb || s.spanType == ext.AppTypeRPC || s.spanType == ""
}

func (s spanSnapshot) getName() string {
	return s.name
}

func (s spanSnapshot) getSpanType() string {
	return s.spanType
}

func (s spanSnapshot) getResource() string {
	return s.resource
}

func (s spanSnapshot) getService() string {
	return s.service
}

func (s spanSnapshot) getDuration() int64 {
	return s.duration
}

func (s spanSnapshot) getSpanID() uint64 {
	return s.spanID
}

func (s spanSnapshot) getParentID() uint64 {
	return s.parentID
}

func (s spanSnapshot) getTraceID() uint64 {
	return s.traceID
}

func (s spanSnapshot) getStartTime() int64 {
	return s.start
}

func (s spanSnapshot) getErrorStatus() int32 {
	return s.errorStatus
}

func (s spanSnapshot) getSpanLinks() []SpanLink {
	return s.spanLinks
}

func (s spanSnapshot) getSpanEvents() []spanEvent {
	return s.spanEvents
}

func (s spanSnapshot) fetchMetadatum(key string) string {
	return s.metas[key]
}

func (s spanSnapshot) getMetadatum(key string) (string, bool) {
	v, ok := s.metas[key]
	return v, ok
}

func (s spanSnapshot) getMetadata() map[string]string {
	return s.metas
}

func (s spanSnapshot) getMetaStruct() metaStructMap {
	return s.metaStruct
}

func (s spanSnapshot) getMetrics() map[string]float64 {
	return s.metrics
}

func (s spanSnapshot) fetchMetric(key string) float64 {
	return s.metrics[key]
}

func (s spanSnapshot) getMetric(key string) (float64, bool) {
	v, ok := s.metrics[key]
	return v, ok
}
