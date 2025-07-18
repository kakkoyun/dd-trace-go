// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"context"

	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
)

type readOnlySpan interface {
	// private:
	isResourcePIISafe() bool

	getName() string
	getSpanType() string
	getResource() string
	getService() string
	getDuration() int64
	getSpanID() uint64
	getTraceID() uint64
	getParentID() uint64
	getStartTime() int64
	getErrorStatus() int32

	fetchMetadatum(key string) string
	getMetadatum(key string) (string, bool)
	getMetadata() map[string]string

	getMetaStruct() metaStructMap
	getMetrics() map[string]float64

	// TODO(kakkoyun): Eliminate test only accessors.
	// Test only accessors:
	getSpanLinks() []SpanLink
	getSpanEvents() []spanEvent
	fetchMetric(key string) float64
	getMetric(key string) (float64, bool)
}

type profilingSpan interface {
	getActivePprofContext() context.Context
	setActivePprofContext(ctx context.Context)
	saveRestorePprofContext(ctx context.Context)
}

type recordingSpan interface {
	readOnlySpan
	profilingSpan

	// Accessors:
	Context() *SpanContext
	BaggageItem(key string) string
	Root() *Span

	// Lifecycle:
	StartChild(operationName string, opts ...StartSpanOption) *Span
	Finish(opts ...FinishOption)

	// Mutating methods:
	SetBaggageItem(key, val string)
	SetOperationName(operationName string)
	SetTag(key string, value interface{})
	SetUser(id string, opts ...UserMonitoringOption)

	AddEvent(name string, opts ...SpanEventOption)
	AddLink(link SpanLink)

	// private:
	isFinished() bool
	isFinishedWhileLocked() bool
	markFinished()
	snapshot() spanSnapshot

	setTraceTags(tags map[string]string)

	fetchAndDeleteMetadatum(key string) string
	setMetadatum(key, v string)

	setMetric(key string, v float64)
	deleteMetric(key string)

	setSamplingPriority(priority int, sampler samplernames.SamplerName)
	setPeerService(peerServiceDefaults bool, peerServiceMappings map[string]string)

	// TODO(kakkoyun): Eliminate test only accessors.
	// Test only accessors:
	setService(service string)
	setTraceID(traceID uint64)
	setStartTime(start int64)
}
