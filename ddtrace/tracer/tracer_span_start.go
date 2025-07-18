// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	gocontext "context"
	"encoding/binary"
	"runtime/pprof"
	rt "runtime/trace"
	"strconv"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	globalinternal "github.com/DataDog/dd-trace-go/v2/internal"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/traceprof"
)

func spanStart(operationName string, options ...StartSpanOption) *Span {
	var opts StartSpanConfig
	for _, fn := range options {
		if fn == nil {
			continue
		}
		fn(&opts)
	}

	// Extract basic span information
	var startTime int64
	if opts.StartTime.IsZero() {
		startTime = now()
	} else {
		startTime = opts.StartTime.UnixNano()
	}

	var context *SpanContext
	// The default pprof context is taken from the start options and is
	// not nil when using StartSpanFromContext()
	pprofContext := opts.Context
	if opts.Parent != nil {
		context = opts.Parent
		if pprofContext == nil && context.span != nil {
			// Inherit the context.Context from parent span if it was propagated
			// using ChildOf() rather than StartSpanFromContext(), see
			// applyPPROFLabels() below.
			pprofContext = context.span.getActivePprofContext()
		}
	}
	if pprofContext == nil {
		// For root span's without context, there is no pprofContext, but we need
		// one to avoid a panic() in pprof.WithLabels(). Using context.Background()
		// is not ideal here, as it will cause us to remove all labels from the
		// goroutine when the span finishes. However, the alternatives of not
		// applying labels for such spans or to leave the endpoint/hotspot labels
		// on the goroutine after it finishes are even less appealing. We'll have
		// to properly document this for users.
		pprofContext = gocontext.Background()
	}

	// Generate span ID
	id := opts.SpanID
	if id == 0 {
		id = generateSpanID(startTime)
	}

	// Initialize span fields with computed values
	var (
		name        = operationName
		service     = ""
		resource    = operationName
		spanID      = id
		traceID     = id
		parentID    uint64
		integration = "manual"
		meta        = make(map[string]string, 8)  // initialize meta map to avoid nil map panic
		metrics     = make(map[string]float64, 4) // initialize metrics map to avoid nil map panic
		spanLinks   = make([]SpanLink, 0, len(opts.SpanLinks))
	)

	// Copy span links from options
	spanLinks = append(spanLinks, opts.SpanLinks...)

	// Handle parent context logic
	if context != nil && !context.getBaggageOnly() {
		// this is a child span
		traceID = context.traceID.Lower()
		parentID = context.spanID
		if p, ok := context.SamplingPriority(); ok {
			metrics[keySamplingPriority] = float64(p)
		}
		if context.span != nil {
			// local parent, inherit service
			service = context.span.getService()
		} else {
			// remote parent
			if origin := context.getOrigin(); origin != "" {
				// mark origin
				meta[keyOrigin] = origin
			}
		}

		if context.reparentID != "" {
			meta[keyReparentID] = context.reparentID
		}
	}

	// Add language tag
	meta["language"] = "go"

	// Create the span with all computed values
	span := &Span{
		name:        name,
		service:     service,
		resource:    resource,
		spanID:      spanID,
		traceID:     traceID,
		parentID:    parentID,
		start:       startTime,
		integration: integration,
		meta:        meta,
		metrics:     metrics,
		spanLinks:   spanLinks,
	}

	// Create span context after span creation
	span.context = newSpanContext(span, context)

	// Add tags from options (must be done after span creation to use SetTag properly)
	for k, v := range opts.Tags {
		span.SetTag(k, v)
	}

	// Handle profiling and tracing setup
	isRootSpan := context == nil || context.span == nil
	if isRootSpan {
		traceprof.SetProfilerRootTags(span)
	}
	if isRootSpan || (context != nil && context.span != nil && context.span.getService() != service) {
		// The span is the local root span.
		span.metrics[keyTopLevel] = 1
		// all top level spans are measured. So the measured tag is redundant.
		delete(span.metrics, keyMeasured)
	}

	// Handle execution tracing
	pprofContext, taskEnd := startExecutionTracerTask(pprofContext, span)
	span.pprofCtxRestore = pprofContext
	span.taskEnd = taskEnd

	return span
}

// StartSpan creates, starts, and returns a new Span with the given `operationName`.
func (t *tracer) StartSpan(operationName string, options ...StartSpanOption) *Span {
	if !t.config.enabled.get() {
		return nil
	}
	span := spanStart(operationName, options...)

	// Collect all config values and compute final state
	var (
		service                    = span.getService()
		serviceName                = t.config.serviceName
		hostname                   = t.config.hostname
		version                    = t.config.version
		env                        = t.config.env
		noDebugStack               = t.config.noDebugStack
		supportsEvents             = t.config.agent.spanEventsAvailable
		universalVersion           = t.config.universalVersion
		serviceMappings            = t.config.serviceMappings
		globalTags                 = t.config.globalTags.get()
		profilerHotspots           = t.config.profilerHotspots
		profilerEndpoints          = t.config.profilerEndpoints
		debugAbandonedSpans        = t.config.debugAbandonedSpans
		spanAttributeSchemaVersion = t.config.spanAttributeSchemaVersion
		pid                        = t.pid
	)

	// Apply service name if empty
	if service == "" {
		service = serviceName
	}

	// Apply service mappings
	if serviceMappings != nil {
		if newSvc, ok := serviceMappings[service]; ok {
			service = newSvc
		}
	}

	// Apply computed values to span
	span.setService(service)
	span.mu.Lock()
	span.noDebugStack = noDebugStack
	span.supportsEvents = supportsEvents
	span.mu.Unlock()

	// Add hostname if provided
	if hostname != "" {
		span.setMetadatum(keyHostname, hostname)
	}

	// Add global tags
	for k, v := range globalTags {
		span.SetTag(k, v)
	}

	// Add version if provided
	if version != "" {
		if universalVersion || (!universalVersion && service == serviceName) {
			span.setMetadatum(ext.Version, version)
		}
	}

	// Add environment if provided
	if env != "" {
		span.setMetadatum(ext.Environment, env)
	}

	// Handle sampling
	if _, ok := span.Context().SamplingPriority(); !ok {
		// if not already sampled or a brand new trace, sample it
		t.sample(span)
	}

	// Apply service mappings again (duplicated in original code)
	if serviceMappings != nil {
		currentService := span.getService()
		if newSvc, ok := serviceMappings[currentService]; ok {
			span.setService(newSvc)
		}
	}

	// Debug logging
	if log.DebugEnabled() {
		// avoid allocating the ...interface{} argument if debug logging is disabled
		spanName := span.getName()
		spanResource := span.getResource()
		spanMeta := span.getMetadata()
		spanMetrics := span.getMetrics()
		log.Debug("Started Span: %v, Operation: %s, Resource: %s, Tags: %v, %v", //nolint:gocritic // Debug logging needs full span representation
			span, spanName, spanResource, spanMeta, spanMetrics)
	}

	// Handle profiler labels
	if profilerHotspots || profilerEndpoints {
		span.mu.Lock()
		pprofCtxRestore := span.pprofCtxRestore
		span.mu.Unlock()
		t.applyPPROFLabels(pprofCtxRestore, span)
	} else {
		span.mu.Lock()
		span.pprofCtxRestore = nil
		span.mu.Unlock()
	}

	// Handle abandoned spans debugging
	if debugAbandonedSpans {
		select {
		case t.abandonedSpansDebugger.In <- newAbandonedSpanCandidate(span, false):
			// ok
		default:
			log.Error("Abandoned spans channel full, disregarding span.")
		}
	}

	// Handle top level span metrics
	if metrics := span.getMetrics(); metrics[keyTopLevel] == 1 {
		// The span is the local root span.
		span.setMetric(keySpanAttributeSchemaVersion, float64(spanAttributeSchemaVersion))
	}

	// Set process ID
	span.setMetric(ext.Pid, float64(pid))

	// Update span statistics
	span.mu.Lock()
	integration := span.integration
	span.mu.Unlock()
	t.spansStarted.Inc(integration)

	return span
}

// applyPPROFLabels applies pprof labels for the profiler's code hotspots and
// endpoint filtering feature to span. When span finishes, any pprof labels
// found in ctx are restored. Additionally, this func informs the profiler how
// many times each endpoint is called.
func (t *tracer) applyPPROFLabels(ctx gocontext.Context, span recordingSpan) {
	// Important: The label keys are ordered alphabetically to take advantage of
	// an upstream optimization that landed in go1.24.  This results in ~10%
	// better performance on BenchmarkStartSpan. See
	// https://go-review.googlesource.com/c/go/+/574516 for more information.
	labels := make([]string, 0, 3*2 /* 3 key value pairs */)
	localRootSpan := span.Root()
	if t.config.profilerHotspots && localRootSpan != nil {
		labels = append(labels, traceprof.LocalRootSpanID, strconv.FormatUint(localRootSpan.getSpanID(), 10))
	}
	if t.config.profilerHotspots {
		labels = append(labels, traceprof.SpanID, strconv.FormatUint(span.getSpanID(), 10))
	}
	if t.config.profilerEndpoints && localRootSpan != nil {
		localRootSpan.mu.Lock()
		if localRootSpan.isResourcePIISafeWhileLocked() {
			labels = append(labels, traceprof.TraceEndpoint, localRootSpan.resource)
			if span == localRootSpan {
				// Inform the profiler of endpoint hits. This is used for the unit of
				// work feature. We can't use APM stats for this since the stats don't
				// have enough cardinality (e.g. runtime-id tags are missing).
				traceprof.GlobalEndpointCounter().Inc(localRootSpan.resource)
			}
		}
		localRootSpan.mu.Unlock()
	}
	if len(labels) > 0 {
		span.saveRestorePprofContext(ctx)
		span.setActivePprofContext(pprof.WithLabels(ctx, pprof.Labels(labels...)))
		pprof.SetGoroutineLabels(span.getActivePprofContext())
	}
}

func startExecutionTracerTask(ctx gocontext.Context, span recordingSpan) (gocontext.Context, func()) {
	if !rt.IsEnabled() {
		return ctx, func() {}
	}
	// TODO(kakkoyun): !! Make sure this is captured in the builder.
	// span.setGoExecTraced(true)

	// Task name is the resource (operationName) of the span, e.g.
	// "POST /foo/bar" (http) or "/foo/pkg.Method" (grpc).
	taskName := span.getResource()
	// If the resource could contain PII (e.g. SQL query that's not using bind
	// arguments), play it safe and just use the span type as the taskName,
	// e.g. "sql".
	if !span.isResourcePIISafe() {
		taskName = span.getSpanType()
	}
	// The task name is an arbitrary string from the user. If it's too
	// large, like a big SQL query, the execution tracer can crash when we
	// create the task. Cap it at an arbirary length.  For "normal" task
	// names this should be plenty that we can still have the task names for
	// debugging.
	taskName = taskName[:min(128, len(taskName))]
	end := noopTaskEnd
	if !globalinternal.IsExecutionTraced(ctx) {
		var task *rt.Task
		ctx, task = rt.NewTask(ctx, taskName)
		end = task.End
	} else {
		// We only want to skip task creation for this particular span,
		// not necessarily for child spans which can come from different
		// integrations. So update this context to be "not" execution
		// traced so that derived contexts used by child spans don't get
		// skipped.
		ctx = globalinternal.WithExecutionNotTraced(ctx)
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], span.getSpanID())
	// TODO: can we make string(b[:]) not allocate? e.g. with unsafe
	// shenanigans? rt.Log won't retain the message string, though perhaps
	// we can't assume that will always be the case.
	rt.Log(ctx, "datadog.uint64_span_id", string(b[:]))
	return ctx, end
}

func noopTaskEnd() {}
