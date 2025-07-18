// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"maps"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/internal/tracerstats"
	"github.com/DataDog/dd-trace-go/v2/internal"
	sharedinternal "github.com/DataDog/dd-trace-go/v2/internal"
	"github.com/DataDog/dd-trace-go/v2/internal/locking"
	"github.com/DataDog/dd-trace-go/v2/internal/locking/assert"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
	"github.com/DataDog/dd-trace-go/v2/internal/telemetry"
)

// trace contains shared context information about a trace, such as sampling
// priority, the root reference and a buffer of the spans which are part of the
// trace, if these exist.
type trace struct {
	// root specifies the root of the trace, if known; it is nil when a span
	// context is extracted from a carrier, at which point there are no spans in
	// the trace yet.
	root recordingSpan // TODO(kakkoyun): !! Make sure this is only set while constructing a trace. Or protect with a mutex.

	mu locking.RWMutex // guards below fields
	// +checklocks:mu
	spans []recordingSpan // all the spans that are part of this trace
	// +checklocks:mu
	tags map[string]string // trace level tags
	// +checklocks:mu
	propagatingTags map[string]string // trace level tags that will be propagated across service boundaries
	// +checklocks:mu
	finishedSpans int // the number of finished spans
	// +checklocks:mu
	full bool // signifies that the span buffer is full
	// +checklocks:mu
	priority *float64 // sampling priority
	// +checklocks:mu
	locked bool // specifies if the sampling priority can be altered
	// +checkatomic
	samplingDecision samplingDecision // samplingDecision indicates whether to send the trace to the agent.
}

var (
	// traceStartSize is the initial size of our trace buffer,
	// by default we allocate for a handful of spans within the trace,
	// reasonable as span is actually way bigger, and avoids re-allocating
	// over and over. Could be fine-tuned at runtime.
	traceStartSize = 10
	// traceMaxSize is the maximum number of spans we keep in memory for a
	// single trace. This is to avoid memory leaks. If more spans than this
	// are added to a trace, then the trace is dropped and the spans are
	// discarded. Adding additional spans after a trace is dropped does
	// nothing.
	traceMaxSize = int(1e5)
)

// newTrace creates a new trace using the given callback which will be called
// upon completion of the trace.
func newTrace() *trace {
	return &trace{spans: make([]recordingSpan, 0, traceStartSize)}
}

func (t *trace) getSpans() []recordingSpan {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.spans
}

func (t *trace) getPropagatingTags() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.propagatingTags
}

func (t *trace) setPropagatingTags(propagatingTags map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.propagatingTags = propagatingTags
}

func (t *trace) getSamplingDecision() samplingDecision {
	return samplingDecision(atomic.LoadUint32((*uint32)(&t.samplingDecision)))
}

func (t *trace) samplingPriority() (p int, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.samplingPriorityWhileLocked()
}

// samplingPriorityWhileLocked returns the sampling priority and true if it is set.
// +checklocksread:t.mu
func (t *trace) samplingPriorityWhileLocked() (p int, ok bool) {
	// TODO(kakkoyun): mutexasserts: abandonedspans_test.go:87
	// mutexassertsAssertMutexLocked(&t.mu)
	if t.priority == nil {
		return 0, false
	}
	return int(*t.priority), true
}

// setSamplingPriority sets the sampling priority and the decision maker
// and returns true if it was modified.
func (t *trace) setSamplingPriority(p int, sampler samplernames.SamplerName) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.setSamplingPriorityWhileLocked(p, sampler)
}

func (t *trace) keep() {
	atomic.CompareAndSwapUint32((*uint32)(&t.samplingDecision), uint32(decisionNone), uint32(decisionKeep))
}

func (t *trace) drop() {
	atomic.CompareAndSwapUint32((*uint32)(&t.samplingDecision), uint32(decisionNone), uint32(decisionDrop))
}

func (t *trace) getTags() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tags
}

func (t *trace) getTag(key string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tags[key]
}

func (t *trace) setTag(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setTagWhileLocked(key, value)
}

// +checklocks:t.mu
func (t *trace) setTagWhileLocked(key, value string) {
	assert.RWMutexLocked(&t.mu)

	if t.tags == nil {
		t.tags = make(map[string]string, 1)
	}
	t.tags[key] = value
}

func samplerToDM(sampler samplernames.SamplerName) string {
	return "-" + strconv.Itoa(int(sampler))
}

func (t *trace) getPriority() *float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.priority
}

func (t *trace) setPriority(p *float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.priority = p
}

// +checklocks:t.mu
func (t *trace) setSamplingPriorityWhileLocked(p int, sampler samplernames.SamplerName) bool {
	assert.RWMutexLocked(&t.mu)

	if t.locked {
		return false
	}

	updatedPriority := t.priority == nil || *t.priority != float64(p)

	if t.priority == nil {
		t.priority = new(float64)
	}
	*t.priority = float64(p)
	curDM, existed := t.propagatingTags[keyDecisionMaker]
	if p > 0 && sampler != samplernames.Unknown {
		// We have a positive priority and the sampling mechanism isn't set.
		// Send nothing when sampler is `Unknown` for RFC compliance.
		// If a global sampling rate is set, it was always applied first. And this call can be
		// triggered again by applying a rule sampler. The sampling priority will be the same, but
		// the decision maker will be different. So we compare the decision makers as well.
		// Note that once global rate sampling is deprecated, we no longer need to compare
		// the DMs. Sampling priority is sufficient to distinguish a change in DM.
		dm := samplerToDM(sampler)
		updatedDM := !existed || dm != curDM
		if updatedDM {
			t.setPropagatingTagWhileLocked(keyDecisionMaker, dm)
			return true
		}
	}
	if p <= 0 && existed {
		delete(t.propagatingTags, keyDecisionMaker)
	}

	return updatedPriority
}

// TODO(kakkoyun): !! Re-evaluate this. Is this elegant enough?
// attemptSamplingUpdate performs the given sampling operation if the trace
// is not locked. Returns true if the operation was performed.
// This encapsulates the check-then-act pattern to avoid exposing lock state.
func (t *trace) attemptSamplingUpdate(samplingOp func()) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locked {
		return false
	}
	samplingOp()
	return true
}

// TODO(kakkoyun): !! Re-evaluate this. Is this elegant enough?
// attemptRulesSampling applies rules sampling if the trace is not locked
// and the decision maker is not manual keep (-4). Returns true if sampling was applied.
func (t *trace) attemptRulesSampling(rulesSampler *rulesSampler) bool {
	t.mu.Lock()
	if t.locked || t.propagatingTags[keyDecisionMaker] == "-4" {
		t.mu.Unlock()
		return false
	}
	root := t.root
	t.mu.Unlock()

	// Call SampleTrace outside the lock to avoid deadlock
	if rulesSampler.SampleTrace(root) {
		t.mu.Lock()
		// Re-check conditions after reacquiring lock
		if !t.locked && t.propagatingTags[keyDecisionMaker] != "-4" {
			t.locked = true
			t.mu.Unlock()
			return true
		}
		t.mu.Unlock()
		return false
	}
	return false
}

// push pushes a new span into the trace. If the buffer is full, it returns
// a errBufferFull error.
func (t *trace) push(s recordingSpan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		return
	}

	tr := getGlobalTracer()
	if len(t.spans) >= traceMaxSize {
		// capacity is reached, we will not be able to complete this trace.
		t.full = true
		t.spans = nil // allow our spans to be collected by GC.
		log.Error("trace buffer full (%d spans), dropping trace", traceMaxSize)
		if tr != nil {
			tracerstats.Signal(tracerstats.TracesDropped, 1)
		}
		return
	}
	t.spans = append(t.spans, s)
	if tr != nil {
		tracerstats.Signal(tracerstats.SpanStarted, 1)
	}
}

func (t *trace) isFirstSpan(s recordingSpan) bool {
	return s == t.getSpans()[0]
}

// +checklocks:t.mu
func (t *trace) isFirstSpanWhileLocked(s recordingSpan) bool {
	assert.RWMutexLocked(&t.mu)
	return len(t.spans) > 0 && s == t.spans[0]
}

// onSpanFinished acknowledges that another span in the trace has finished, and checks
// if the trace is complete.
// It uses the given priority, if non-nil, to mark the root span.
// This also will trigger a partial flush if enabled,
// and the total number of finished spans is greater than or equal to the partial flush limit.
func (t *trace) onSpanFinished(s recordingSpan) {
	tr := getGlobalTracer()
	if tr == nil {
		return
	}
	tc := tr.TracerConf()

	// TODO: Find a better pattern for mocktracer.
	// This is here to support the mocktracer. It would be nice to be able to not do this.
	// We need to track when any single span is finished.
	if mtr, ok := tr.(interface{ FinishSpan(*Span) }); ok {
		ss, ok := s.(*Span)
		if !ok {
			// NOTICE: This should never happen.
			panic("recordingSpan is not a *Span")
		}
		mtr.FinishSpan(ss)
	}

	// TODO(kakkoyun): !!  If we mark finished earlier, how are we going to handle these?
	// Do span operations before acquiring trace lock to avoid deadlock
	s.setPeerService(tc.PeerServiceDefaults, tc.PeerServiceMappings)
	// attach the _dd.base_service tag only when the globally configured service name is different from the
	// span service name.
	spanService := s.getService()
	if spanService != "" && !strings.EqualFold(spanService, tc.ServiceTag) {
		s.setMetadatum(keyBaseService, tc.ServiceTag)
	}

	// TODO(kakkoyun): !! Move this to "finish" method in span.go!
	// Ensure the span is marked as finished.
	// This is a no-op if the span is already finished.
	s.markFinished()

	// Collect operations to be done on spans while holding trace lock
	var spanOperations []func()

	t.mu.Lock()

	// Collect operations while holding lock
	if s == t.root && t.priority != nil {
		// Store priority value for use outside lock
		priority := *t.priority
		spanOperations = append(spanOperations, func() {
			t.root.setMetric(keySamplingPriority, priority)
		})
		t.locked = true
	}

	if t.isFirstSpanWhileLocked(s) {
		// Collect trace tags for use outside lock
		tags := make(map[string]string, len(t.tags)+len(t.propagatingTags)+len(sharedinternal.GetTracerGitMetadataTags()))
		maps.Copy(tags, t.tags)
		maps.Copy(tags, t.propagatingTags)
		maps.Copy(tags, sharedinternal.GetTracerGitMetadataTags())

		spanOperations = append(spanOperations, func() {
			s.setTraceTags(tags)
		})
	}

	// Continue with trace-locked operations (non-span operations)
	if t.full {
		// capacity has been reached, the buffer is no longer tracking
		// all the spans in the trace, so the below conditions will not
		// be accurate and would trigger a pre-mature flush, exposing us
		// to a race condition where spans can be modified while flushing.
		//
		// TODO(partialFlush): should we do a partial flush in this scenario?
		t.mu.Unlock()

		// Execute span operations outside the lock
		for _, op := range spanOperations {
			op()
		}
		return
	}
	t.finishedSpans++

	// Handle full flush case
	if len(t.spans) == t.finishedSpans {
		// Copy spans data before releasing lock
		recordingSpans := make([]recordingSpan, len(t.spans))
		for i, span := range t.spans {
			recordingSpans[i] = span
		}
		willSend := decisionKeep == samplingDecision(atomic.LoadUint32((*uint32)(&t.samplingDecision)))
		t.spans = nil
		t.mu.Unlock()

		// Execute span operations outside the lock
		for _, op := range spanOperations {
			op()
		}

		// Submit chunk
		if tr, ok := tr.(*tracer); ok {
			tr.submitChunk(&chunk{
				spans:    recordingSpans,
				willSend: willSend,
			})
		}
		return
	}

	// Handle partial flush
	doPartialFlush := tc.PartialFlush && t.finishedSpans >= tc.PartialFlushMinSpans
	if !doPartialFlush {
		t.mu.Unlock()

		// Execute span operations outside the lock
		for _, op := range spanOperations {
			op()
		}
		return
	}

	// Partial flush logic
	log.Debug("Partial flush triggered with %d finished spans", t.finishedSpans)
	telemetry.Count(
		telemetry.NamespaceTracers,
		"trace_partial_flush.count",
		[]string{"reason:large_trace"},
	).Submit(1)

	var (
		finishedSpans = make([]recordingSpan, 0, t.finishedSpans)
		leftoverSpans = make([]recordingSpan, 0, max(0, len(t.spans)-t.finishedSpans))
	)
	for _, span := range t.spans {
		if span.isFinishedWhileLocked() {
			finishedSpans = append(finishedSpans, span)
		} else {
			leftoverSpans = append(leftoverSpans, span)
		}
	}

	// Collect additional span operations for partial flush
	if len(finishedSpans) > 0 {
		firstFinishedSpan := finishedSpans[0]
		priority := *t.priority
		spanOperations = append(spanOperations, func() {
			firstFinishedSpan.setMetric(keySamplingPriority, priority)
		})

		if s != t.spans[0] {
			// Collect trace tags for first finished span
			tags := make(map[string]string, len(t.tags)+len(t.propagatingTags)+len(sharedinternal.GetTracerGitMetadataTags()))
			maps.Copy(tags, t.tags)
			maps.Copy(tags, t.propagatingTags)
			maps.Copy(tags, sharedinternal.GetTracerGitMetadataTags())

			spanOperations = append(spanOperations, func() {
				firstFinishedSpan.setTraceTags(tags)
			})
		}
	}

	willSend := decisionKeep == samplingDecision(atomic.LoadUint32((*uint32)(&t.samplingDecision)))
	t.spans = leftoverSpans
	t.mu.Unlock()

	// Execute span operations outside the lock
	for _, op := range spanOperations {
		op()
	}

	// Submit telemetry and chunk
	telemetry.Distribution(
		telemetry.NamespaceTracers,
		"trace_partial_flush.spans_closed",
		nil,
	).Submit(float64(len(finishedSpans)))
	telemetry.Distribution(
		telemetry.NamespaceTracers,
		"trace_partial_flush.spans_remaining",
		nil,
	).Submit(float64(len(leftoverSpans)))

	if tr, ok := tr.(*tracer); ok {
		recordingSpans := make([]recordingSpan, len(finishedSpans))
		for i, span := range finishedSpans {
			recordingSpans[i] = span
		}
		tr.submitChunk(&chunk{
			spans:    recordingSpans,
			willSend: willSend,
		})
	}
}

// setTraceTags sets all "trace level" tags on the provided span
// +checklocks:t.mu
func (t *trace) setTraceTags(s recordingSpan) {
	assert.RWMutexLocked(&t.mu)

	tags := make(map[string]string, len(t.tags)+len(t.propagatingTags)+len(sharedinternal.GetTracerGitMetadataTags()))
	maps.Copy(tags, t.tags)
	maps.Copy(tags, t.propagatingTags)
	maps.Copy(tags, sharedinternal.GetTracerGitMetadataTags())
	s.setTraceTags(tags)
}

// +checklocks:t.mu
func (t *trace) finishChunkWhileLocked(tr *tracer, ch *chunk) {
	assert.RWMutexLocked(&t.mu)

	tr.submitChunk(ch)
	t.finishedSpans = 0 // important, because a buffer can be used for several flushes.
}

func (t *trace) hasPropagatingTag(k string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.propagatingTags[k]
	return ok
}

func (t *trace) propagatingTag(k string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.propagatingTags[k]
}

// setPropagatingTag sets the key/value pair as a trace propagating tag.
func (t *trace) setPropagatingTag(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setPropagatingTagWhileLocked(key, value)
}

func (t *trace) setTraceSourcePropagatingTag(key string, value internal.TraceSource) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If there is already a TraceSource value set in the trace
	// we need to add the new value to the bitmask.
	if source := t.propagatingTags[key]; source != "" {
		tSource, err := internal.ParseTraceSource(source)
		if err != nil {
			log.Error("failed to parse trace source tag: %v", err.Error())
		}

		tSource |= value

		t.setPropagatingTagWhileLocked(key, tSource.String())
		return
	}

	t.setPropagatingTagWhileLocked(key, value.String())
}

// setPropagatingTagWhileLocked sets the key/value pair as a trace propagating tag.
// Not safe for concurrent use, setPropagatingTag should be used instead in that case.
// +checklocks:t.mu
func (t *trace) setPropagatingTagWhileLocked(key, value string) {
	assert.RWMutexLocked(&t.mu)

	if t.propagatingTags == nil {
		t.propagatingTags = make(map[string]string, 1)
	}
	t.propagatingTags[key] = value
}

// unsetPropagatingTag deletes the key/value pair from the trace's propagated tags.
func (t *trace) unsetPropagatingTag(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.propagatingTags, key)
}

// iteratePropagatingTags allows safe iteration through the propagating tags of a trace.
// the trace must not be modified during this call, as it is locked for reading.
//
// f should return whether the iteration should continue.
func (t *trace) iteratePropagatingTags(f func(k, v string) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, v := range t.propagatingTags {
		if !f(k, v) {
			break
		}
	}
}

func (t *trace) replacePropagatingTags(tags map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.propagatingTags = tags
}

func (t *trace) propagatingTagsLen() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.propagatingTags)
}
