// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace"
	sharedinternal "github.com/DataDog/dd-trace-go/v2/internal"
	"github.com/DataDog/dd-trace-go/v2/internal/locking"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
)

const TraceIDZero string = "00000000000000000000000000000000"

var _ ddtrace.SpanContext = (*SpanContext)(nil)

type traceID [16]byte // traceID in big endian, i.e. <upper><lower>

var emptyTraceID traceID

func (t *traceID) HexEncoded() string {
	return hex.EncodeToString(t[:])
}

func (t *traceID) Lower() uint64 {
	return binary.BigEndian.Uint64(t[8:])
}

func (t *traceID) Upper() uint64 {
	return binary.BigEndian.Uint64(t[:8])
}

func (t *traceID) SetLower(i uint64) {
	binary.BigEndian.PutUint64(t[8:], i)
}

func (t *traceID) SetUpper(i uint64) {
	binary.BigEndian.PutUint64(t[:8], i)
}

func (t *traceID) SetUpperFromHex(s string) error {
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return fmt.Errorf("malformed %q: %s", s, err)
	}
	t.SetUpper(u)
	return nil
}

func (t *traceID) Empty() bool {
	return *t == emptyTraceID
}

func (t *traceID) HasUpper() bool {
	for _, b := range t[:8] {
		if b != 0 {
			return true
		}
	}
	return false
}

func (t *traceID) UpperHex() string {
	return hex.EncodeToString(t[:8])
}

// SpanContext represents a span state that can propagate to descendant spans
// and across process boundaries. It contains all the information needed to
// spawn a direct descendant of the span that it belongs to. It can be used
// to create distributed tracing by propagating it using the provided interfaces.
type SpanContext struct {
	// the below group should propagate only locally

	// TODO(kakkoyun): !! Make sure this is only set while constructing a trace.
	trace *trace        // reference to the trace that this span belongs to.
	span  recordingSpan // reference to the span that hosts this context.

	// +checkatomic
	errors atomic.Int32 // number of spans with errors in this trace.

	// The 16-character hex string of the last seen Datadog Span ID
	// this value will be added as the _dd.parent_id tag to spans
	// created from this spanContext.
	// This value is extracted from the `p` sub-key within the tracestate.
	// The backend will use the _dd.parent_id tag to reparent spans in
	// distributed traces if they were missing their parent span.
	// Missing parent span could occur when a W3C-compliant tracer
	// propagated this context, but didn't send any spans to Datadog.
	reparentID string
	isRemote   bool

	// the below group should propagate cross-process

	traceID traceID
	spanID  uint64

	// guards below fields.
	mu locking.RWMutex
	// +checklocks:mu
	baggage map[string]string
	// +checkatomic
	hasBaggage uint32 // atomic int for quick checking presence of baggage. 0 indicates no baggage, otherwise baggage exists.
	// +checklocks:mu
	origin string // e.g. "synthetics"
	// +checklocks:mu
	spanLinks []SpanLink // links to related spans in separate|external|disconnected traces
	// +checklocks:mu
	updated bool // updated is tracking changes for priority / origin / x-datadog-tags
	// +checklocks:mu
	baggageOnly bool // when true, indicates this context only propagates baggage items and should not be used for distributed tracing fields
}

// Private interface for converting v1 span contexts to v2 ones.
type spanContextV1Adapter interface {
	SamplingDecision() uint32
	Origin() string
	Priority() *float64
	PropagatingTags() map[string]string
	Tags() map[string]string
}

// FromGenericCtx converts a ddtrace.SpanContext to a *SpanContext, which can be used
// to start child spans.
func FromGenericCtx(c ddtrace.SpanContext) *SpanContext {
	var sc SpanContext
	sc.traceID = c.TraceIDBytes()
	sc.spanID = c.SpanID()
	sc.baggage = make(map[string]string)
	c.ForeachBaggageItem(func(k, v string) bool {
		atomic.StoreUint32(&sc.hasBaggage, 1)
		sc.baggage[k] = v // +checklocksignore: mu is locked for ForeachBaggageItem.
		return true
	})
	ctx, ok := c.(spanContextV1Adapter)
	if !ok {
		return &sc
	}
	sc.origin = ctx.Origin()
	sc.trace = newTrace()
	sc.trace.setPriority(ctx.Priority())
	atomic.StoreUint32((*uint32)(&sc.trace.samplingDecision), uint32(ctx.SamplingDecision()))
	// Set tags individually since there's no bulk setter
	tags := ctx.Tags()
	for k, v := range tags {
		sc.trace.setTag(k, v)
	}
	sc.trace.setPropagatingTags(ctx.PropagatingTags())
	return &sc
}

// newSpanContext creates a new SpanContext to serve as context for the given
// span. If the provided parent is not nil, the context will inherit the trace,
// baggage and other values from it. This method also pushes the span into the
// new context's trace and as a result, it should not be called multiple times
// for the same span.
func newSpanContext(span recordingSpan, parent *SpanContext) *SpanContext {
	var (
		spanID      = span.getSpanID()
		spanTraceID = span.getTraceID()
		fullTraceID traceID

		trace       *trace
		origin      string
		errors      int32
		baggage     map[string]string
		hasBaggage  uint32
		baggageOnly bool
	)
	// Initialize traceID with lower part
	fullTraceID.SetLower(spanTraceID)
	switch {
	case parent != nil:
		// Read parent fields with single lock acquisition to avoid multiple lock operations.
		parent.mu.RLock()
		if !parent.baggageOnly {
			fullTraceID.SetUpper(parent.traceID.Upper())
			trace = parent.trace
			origin = parent.origin
			errors = parent.errors.Load()
		}
		parent.mu.RUnlock()

		// Collect baggage items
		parent.ForeachBaggageItem(func(k, v string) bool {
			if baggage == nil {
				baggage = make(map[string]string)
			}
			baggage[k] = v
			return true
		})
		if len(baggage) > 0 {
			hasBaggage = 1
		}
	case sharedinternal.BoolEnv("DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED", true):
		// add 128 bit trace id, if enabled, formatted as big-endian:
		// <32-bit unix seconds> <32 bits of zero> <64 random bits>
		id128 := time.Duration(span.getStartTime()) / time.Second
		// casting from int64 -> uint32 should be safe since the start time won't be
		// negative, and the seconds should fit within 32-bits for the foreseeable future.
		// (We only want 32 bits of time, then the rest is zero)
		tUp := uint64(uint32(id128)) << 32 // We need the time at the upper 32 bits of the uint
		fullTraceID.SetUpper(tUp)
	}

	// Initialize trace if not inherited from parent
	if trace == nil {
		trace = newTrace()
	}
	if trace.root == nil {
		// first span in the trace can safely be assumed to be the root
		trace.root = span
	}

	context := &SpanContext{
		// Local reference fields
		trace: trace,
		span:  span,
		// Cross-process propagation fields
		traceID: fullTraceID,
		spanID:  spanID,
		// Mutex-protected fields
		baggage:    baggage,
		hasBaggage: hasBaggage,
		origin:     origin,
		// setting context.updated to false here is necessary to distinguish
		// between initializing properties of the span (priority)
		// and updating them after extracting context through propagators
		updated:     false,
		baggageOnly: baggageOnly,
	}
	context.errors.Store(errors)

	// Extract sampling priority before pushing to avoid deadlock
	// between span lock (in getMetric) and trace lock (in push)
	var samplingPriority *int
	if v, ok := span.getMetric(keySamplingPriority); ok {
		priority := int(v)
		samplingPriority = &priority
	}

	trace.push(span)

	// Apply sampling priority after span is added to trace
	if samplingPriority != nil {
		trace.setSamplingPriority(*samplingPriority, samplernames.Unknown)
	}

	return context
}

// SpanID implements ddtrace.SpanContext.
func (c *SpanContext) SpanID() uint64 {
	if c == nil {
		return 0
	}
	return c.spanID
}

func (c *SpanContext) getOrigin() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.origin
}

func (c *SpanContext) setOrigin(origin string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.origin = origin
}

func (c *SpanContext) getSpanLinks() []SpanLink {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.spanLinks
}

func (c *SpanContext) setSpanLinks(spanLinks []SpanLink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spanLinks = spanLinks
}

func (c *SpanContext) getUpdated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.updated
}

func (c *SpanContext) setUpdated(updated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updated = updated
}

func (c *SpanContext) getBaggageOnly() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggageOnly
}

// TraceID implements ddtrace.SpanContext.
func (c *SpanContext) TraceID() string {
	if c == nil {
		return TraceIDZero
	}
	return c.traceID.HexEncoded()
}

// TraceIDBytes implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDBytes() [16]byte {
	if c == nil {
		return emptyTraceID
	}
	return c.traceID
}

// TraceIDLower implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDLower() uint64 {
	if c == nil {
		return 0
	}
	return c.traceID.Lower()
}

// TraceIDUpper implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDUpper() uint64 {
	if c == nil {
		return 0
	}
	return c.traceID.Upper()
}

// SpanLinks implements ddtrace.SpanContext
func (c *SpanContext) SpanLinks() []SpanLink {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make([]SpanLink, len(c.spanLinks))
	copy(cp, c.spanLinks)
	return cp
}

// ForeachBaggageItem implements ddtrace.SpanContext.
func (c *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	if c == nil {
		return
	}
	if atomic.LoadUint32(&c.hasBaggage) == 0 {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, v := range c.baggage {
		if !handler(k, v) {
			break
		}
	}
}

// sets the sampling priority and decision maker (based on `sampler`).
func (c *SpanContext) setSamplingPriority(p int, sampler samplernames.SamplerName) {
	if c.trace == nil {
		c.trace = newTrace()
	}
	if c.trace.setSamplingPriority(p, sampler) {
		// the trace's sampling priority or sampler was updated: mark this as updated.
		c.setUpdated(true)
	}
}

func (c *SpanContext) SamplingPriority() (p int, ok bool) {
	if c == nil || c.trace == nil {
		return 0, false
	}
	return c.trace.samplingPriority()
}

func (c *SpanContext) getBaggage() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggage
}

func (c *SpanContext) setBaggage(baggage map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baggage = baggage
}

func (c *SpanContext) setBaggageItem(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.baggage == nil {
		atomic.StoreUint32(&c.hasBaggage, 1)
		c.baggage = make(map[string]string, 1)
	}
	c.baggage[key] = val
}

func (c *SpanContext) baggageItem(key string) string {
	if atomic.LoadUint32(&c.hasBaggage) == 0 {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggage[key]
}

// finish marks this span as finished in the trace.
func (c *SpanContext) finish() {
	c.trace.onSpanFinished(c.span)
}

// safeDebugString returns a safe string representation of the SpanContext for debug logging.
// It excludes potentially sensitive data like baggage contents while preserving useful debugging information.
func (c *SpanContext) safeDebugString() string {
	if c == nil {
		return "<nil>"
	}

	hasBaggage := atomic.LoadUint32(&c.hasBaggage) != 0
	var baggageCount int
	var origin, updated, baggageOnly string

	// Single lock acquisition to read all mutex-protected fields
	c.mu.RLock()
	if hasBaggage {
		baggageCount = len(c.baggage)
	}
	origin = c.origin
	updated = fmt.Sprintf("%t", c.updated)
	baggageOnly = fmt.Sprintf("%t", c.baggageOnly)
	c.mu.RUnlock()

	return fmt.Sprintf("SpanContext{traceID=%s, spanID=%d, hasBaggage=%t, baggageCount=%d, origin=%q, updated=%s, isRemote=%t, baggageOnly=%s}",
		c.TraceID(), c.SpanID(), hasBaggage, baggageCount, origin, updated, c.isRemote, baggageOnly)
}

// samplingDecision is the decision to send a trace to the agent or not.
type samplingDecision uint32

const (
	// decisionNone is the default state of a trace.
	// If no decision is made about the trace, the trace won't be sent to the agent.
	decisionNone samplingDecision = iota
	// decisionDrop prevents the trace from being sent to the agent.
	decisionDrop
	// decisionKeep ensures the trace will be sent to the agent.
	decisionKeep
)
