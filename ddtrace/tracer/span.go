// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	rt "runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/datadog-agent/pkg/obfuscate"
	"golang.org/x/xerrors"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/instrumentation/errortrace"
	sharedinternal "github.com/DataDog/dd-trace-go/v2/internal"
	"github.com/DataDog/dd-trace-go/v2/internal/globalconfig"
	"github.com/DataDog/dd-trace-go/v2/internal/locking"
	"github.com/DataDog/dd-trace-go/v2/internal/locking/assert"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/orchestrion"
	"github.com/DataDog/dd-trace-go/v2/internal/processtags"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
	"github.com/DataDog/dd-trace-go/v2/internal/telemetry"
	"github.com/DataDog/dd-trace-go/v2/internal/traceprof"
)

type (
	// spanList implements msgp.Encodable on top of a slice of spans.
	spanList []*Span

	// spanLists implements msgp.Decodable on top of a slice of spanList.
	// This type is only used in tests.
	spanLists []spanList
)

var (
	_ readOnlySpan  = (*Span)(nil)
	_ recordingSpan = (*Span)(nil)
)

// errorConfig holds customization options for setting error tags.
type errorConfig struct {
	noDebugStack bool
	stackFrames  uint
	stackSkip    uint
}

// Span represents a computation. Callers must call Finish when a Span is
// complete to ensure it's submitted.
type Span struct {
	// all fields are protected by this mutex.
	mu locking.Mutex

	// +checklocks:mu
	context *SpanContext // span propagation context

	// +checklocks:mu
	name string // operation name
	// +checklocks:mu
	service string // service name (i.e. "grpc.server", "http.request")
	// +checklocks:mu
	resource string // resource name (i.e. "/user?id=123", "SELECT * FROM users")
	// +checklocks:mu
	spanType string // protocol associated with the span (i.e. "web", "db", "cache")
	// +checklocks:mu
	start int64 // span start time expressed in nanoseconds since epoch
	// +checklocks:mu
	duration int64 // duration of the span expressed in nanoseconds

	// +checklocks:mu
	meta map[string]string // arbitrary map of metadata
	// +checklocks:mu
	metaStruct metaStructMap // arbitrary map of metadata with structured values
	// +checklocks:mu
	metrics map[string]float64 // arbitrary map of numeric metrics

	// +checklocks:mu
	spanID uint64 // identifier of this span
	// +checklocks:mu
	traceID uint64 // lower 64-bits of the root span identifier
	// +checklocks:mu
	parentID uint64 // identifier of the span's direct parent

	// +checklocks:mu
	errorStatus int32 // error status of the span; 0 means no errors

	// +checklocks:mu
	spanLinks []SpanLink // links to other spans
	// +checklocks:mu
	spanEvents []spanEvent // events produced related to this span

	// +checklocks:mu
	goExecTraced bool
	// +checklocks:mu
	noDebugStack bool // disables debug stack traces

	// +checklocks:mu
	integration string // where the span was started from, such as a specific contrib or "manual"
	// +checklocks:mu
	supportsEvents bool // whether the span supports native span events or not

	// +checklocks:mu
	pprofCtxActive context.Context // contains pprof.WithLabel labels to tell the profiler more about this span
	// +checklocks:mu
	pprofCtxRestore context.Context // contains pprof.WithLabel labels of the parent span (if any) that need to be restored when this span finishes

	// true if the span has been submitted to a tracer.
	// Can only be read/modified if the trace is locked.
	// +checklocks:mu
	finished bool

	// ends execution tracer (runtime/trace) task, if started
	// +checklocks:mu
	taskEnd func()
}

// AsMap places tags and span properties into a map and returns it.
//
// Note that this is not performant, nor are spans guaranteed to have all of their
// properties set at any time during normal operation! This is used for testing only,
// and should not be used in non-test code, or you may run into performance or other
// issues.
func (s *Span) AsMap() map[string]interface{} {
	m := make(map[string]interface{})
	if s == nil {
		return m
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m[ext.SpanName] = s.name
	m[ext.ServiceName] = s.service
	m[ext.ResourceName] = s.resource
	m[ext.SpanType] = s.spanType
	m[ext.MapSpanStart] = s.start
	m[ext.MapSpanDuration] = s.duration
	for k, v := range s.meta {
		m[k] = v
	}
	for k, v := range s.metrics {
		m[k] = v
	}
	for k, v := range s.metaStruct {
		m[k] = v
	}
	m[ext.MapSpanID] = s.spanID
	m[ext.MapSpanTraceID] = s.traceID
	m[ext.MapSpanParentID] = s.parentID
	m[ext.MapSpanError] = s.errorStatus
	if events := s.spanEventsAsJSONStringWhileLocked(); events != "" {
		m[ext.MapSpanEvents] = events
	}
	return m
}

func (s *Span) getName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.name
}

func (s *Span) getSpanType() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spanType
}

func (s *Span) getResource() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resource
}

func (s *Span) setResource(resource string) {
	s.mu.Lock()
	s.resource = resource
	s.mu.Unlock()
}

func (s *Span) getService() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.service
}

func (s *Span) setService(service string) {
	s.mu.Lock()
	s.service = service
	s.mu.Unlock()
}

func (s *Span) getDuration() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duration
}

// +checklocks:s.mu
func (s *Span) spanEventsAsJSONStringWhileLocked() string {
	assert.MutexLocked(&s.mu)

	if !s.supportsEvents {
		return s.meta["events"]
	}
	return marshalEvents(s.spanEvents)
}

func marshalEvents(events []spanEvent) string {
	if events == nil {
		return ""
	}
	out, err := json.Marshal(events)
	if err != nil {
		log.Error("failed to marshal span events: %v", err.Error())
		return ""
	}
	return string(out)
}

// Context yields the SpanContext for this Span. Note that the return
// value of Context() is still valid after a call to Finish(). This is
// called the span context and it is different from Go's context.
func (s *Span) Context() *SpanContext {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.context
}

func (s *Span) getSpanID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spanID
}

func (s *Span) getTraceID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traceID
}

func (s *Span) setTraceID(traceID uint64) {
	s.mu.Lock()
	s.traceID = traceID
	s.mu.Unlock()
}

func (s *Span) getParentID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.parentID
}

func (s *Span) setStartTime(start int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.start = start
}

func (s *Span) getStartTime() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.start
}

func (s *Span) setSpanID(spanID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spanID = spanID
}

func (s *Span) setSupportsEvents(supportsEvents bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.supportsEvents = supportsEvents
}

func (s *Span) setContext(context *SpanContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.context = context
}

// SetBaggageItem sets a key/value pair as baggage on the span. Baggage items
// are propagated down to descendant spans and injected cross-process. Use with
// care as it adds extra load onto your tracing layer.
func (s *Span) SetBaggageItem(key, val string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.context.setBaggageItem(key, val)
	s.mu.Unlock()
}

// BaggageItem gets the value for a baggage item given its key. Returns the
// empty string if the value isn't found in this Span.
func (s *Span) BaggageItem(key string) string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.context.baggageItem(key)
}

// SetTag adds a set of key/value metadata to the span.
func (s *Span) SetTag(key string, value interface{}) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.setTagWhileLocked(key, value)
	s.mu.Unlock()
}

// +checklocks:s.mu
func (s *Span) setTagWhileLocked(key string, value interface{}) {
	assert.MutexLocked(&s.mu)

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		return
	}

	// To avoid dumping the memory address in case value is a pointer, we dereference it.
	// Any pointer value that is a pointer to a pointer will be dumped as a string.
	value = dereference(value)

	switch key {
	case ext.Error:
		s.setTagErrorWhileLocked(value, errorConfig{
			noDebugStack: s.noDebugStack,
		})
		return
	case ext.Component:
		integration, ok := value.(string)
		if ok {
			s.integration = integration
		}
	}
	if v, ok := value.(bool); ok {
		s.setTagBoolWhileLocked(key, v)
		return
	}
	if v, ok := value.(string); ok {
		if key == ext.ResourceName && s.pprofCtxActive != nil && s.isResourcePIISafeWhileLocked() {
			// If the user overrides the resource name for the span,
			// update the endpoint label for the runtime profilers.
			//
			// We don't change s.pprofCtxRestore since that should
			// stay as the original parent span context regardless
			// of what we change at a lower level.
			s.pprofCtxActive = pprof.WithLabels(s.pprofCtxActive, pprof.Labels(traceprof.TraceEndpoint, v))
			pprof.SetGoroutineLabels(s.pprofCtxActive)
		}
		s.setMetaWhileLocked(key, v)
		return
	}
	if v, ok := sharedinternal.ToFloat64(value); ok {
		s.setMetricWhileLocked(key, v)
		return
	}
	if v, ok := value.(fmt.Stringer); ok {
		defer func() {
			if e := recover(); e != nil {
				if v := reflect.ValueOf(value); v.Kind() == reflect.Ptr && v.IsNil() {
					// If .String() panics due to a nil receiver, we want to catch this
					// and replace the string value with "<nil>", just as Sprintf does.
					// Other panics should not be handled.
					s.setMetaWhileLocked(key, "<nil>")
					return
				}
				panic(e)
			}
		}()
		s.setMetaWhileLocked(key, v.String())
		return
	}

	if v, ok := value.([]byte); ok {
		s.setMetaWhileLocked(key, string(v))
		return
	}

	if value != nil {
		// Arrays will be translated to dot notation. e.g.
		// {"myarr.0": "foo", "myarr.1": "bar"}
		// which will be displayed as an array in the UI.
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice:
			slice := reflect.ValueOf(value)
			for i := 0; i < slice.Len(); i++ {
				key := fmt.Sprintf("%s.%d", key, i)
				v := slice.Index(i)
				if num, ok := sharedinternal.ToFloat64(v.Interface()); ok {
					s.setMetricWhileLocked(key, num)
				} else {
					s.setMetaWhileLocked(key, fmt.Sprintf("%v", v))
				}
			}
			return
		}

		// Can be sent as messagepack in `meta_struct` instead of `meta`
		// reserved for internal use only
		if v, ok := value.(sharedinternal.MetaStructValue); ok {
			s.setMetaStructWhileLocked(key, v.Value)
			return
		}

		// Support for v1 shim meta struct values (only _dd.stack uses this)
		if key == "_dd.stack" {
			s.setMetaStructWhileLocked(key, value)
			return
		}

		// Add this trace source tag to propagating tags and to span tags
		// reserved for internal use only
		if v, ok := value.(sharedinternal.TraceSourceTagValue); ok {
			s.context.trace.setTraceSourcePropagatingTag(key, v.Value)
		}
	}

	// not numeric, not a string, not a fmt.Stringer, not a bool, and not an error
	s.setMetaWhileLocked(key, fmt.Sprint(value))
}

func (s *Span) setTraceTags(tags map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range tags {
		s.setMetaWhileLocked(k, v)
	}
	if ctx := s.context; ctx != nil && ctx.traceID.HasUpper() {
		s.setMetaWhileLocked(keyTraceID128, ctx.traceID.UpperHex())
	}
	if pTags := processtags.GlobalTags().String(); pTags != "" {
		s.setMetaWhileLocked(keyProcessTags, pTags)
	}
}

// setSamplingPriority locks the span, then updates the sampling priority.
// It also updates the trace's sampling priority.
func (s *Span) setSamplingPriority(priority int, sampler samplernames.SamplerName) {
	if s == nil {
		return
	}
	s.mu.Lock()
	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.setMetricWhileLocked(keySamplingPriority, float64(priority))
	context := s.context // get context reference while holding lock
	s.mu.Unlock()

	// Call trace's setSamplingPriority outside span lock to avoid deadlock
	context.setSamplingPriority(priority, sampler)
}

// root returns the root span of the span's trace. The return value shouldn't be
// nil as long as the root span is valid and not finished.
func (s *Span) Root() *Span {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.context == nil {
		return nil
	}
	root, ok := s.rootWhileLocked().(*Span)
	if !ok {
		// TODO(kakkoyun): This should never happen.
		// Add a log message after thorough testing.
		log.Error("Span.Root: root is not a *Span")
		// return nil
		// TODO(kakkoyun): Add a debugPanic, only in debug mode.
		// Print the stack trace and exit.
		// Could an assert library?
		// debugPanic("Span.Root: root is not a *Span")
		panic("Span.Root: root is not a *Span")
	}
	return root
}

func (s *Span) isRoot() bool {
	// TODO(kakkoyun): How to safely access to context? Is it necessary?
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.context == nil {
		return false
	}
	if s.context.trace == nil {
		return false
	}
	return s.context.trace.root == s
}

// +checklocks:s.mu
func (s *Span) rootWhileLocked() recordingSpan {
	assert.MutexLocked(&s.mu)

	if s.context == nil {
		return nil
	}
	if s.context.trace == nil {
		return nil
	}
	return s.context.trace.root
}

// SetUser associates user information to the current trace which the
// provided span belongs to. The options can be used to tune which user
// bit of information gets monitored. In case of distributed traces,
// the user id can be propagated across traces using the WithPropagation() option.
// See https://docs.datadoghq.com/security_platform/application_security/setup_and_configure/?tab=set_user#add-user-information-to-traces
func (s *Span) SetUser(id string, opts ...UserMonitoringOption) {
	if s == nil {
		return
	}
	cfg := UserMonitoringConfig{
		Metadata: make(map[string]string),
	}
	for _, fn := range opts {
		fn(&cfg)
	}

	root := s.Root()

	root.mu.Lock()
	defer root.mu.Unlock()

	if s != root {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	trace := root.context.trace
	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if root.finished {
		return
	}
	if cfg.PropagateID {
		// Delete usr.id from the tags since _dd.p.usr.id takes precedence
		delete(root.meta, keyUserID)
		idenc := base64.StdEncoding.EncodeToString([]byte(id))
		trace.setPropagatingTag(keyPropagatedUserID, idenc)
		context := s.context // +checklocksignore: Locked with a condition above.
		context.setUpdated(true)
	} else {
		if trace.hasPropagatingTag(keyPropagatedUserID) {
			// Unset the propagated user ID so that a propagated user ID coming from upstream won't be propagated anymore.
			trace.unsetPropagatingTag(keyPropagatedUserID)
			context := s.context // +checklocksignore: Locked with a condition above.
			context.setUpdated(true)
		}
		delete(root.meta, keyPropagatedUserID)
	}

	usrData := map[string]string{
		keyUserID:        id,
		keyUserLogin:     cfg.Login,
		keyUserEmail:     cfg.Email,
		keyUserName:      cfg.Name,
		keyUserScope:     cfg.Scope,
		keyUserRole:      cfg.Role,
		keyUserSessionID: cfg.SessionID,
	}
	for k, v := range cfg.Metadata {
		usrData[fmt.Sprintf("usr.%s", k)] = v
	}
	for k, v := range usrData {
		if v != "" {
			// setMetadatum is used since the span is already locked
			root.setMetaWhileLocked(k, v)
		}
	}
}

// StartChild starts a new child span with the given operation name and options.
func (s *Span) StartChild(operationName string, opts ...StartSpanOption) *Span {
	if s == nil {
		return nil
	}
	opts = append(opts, ChildOf(s.Context()))
	return getGlobalTracer().StartSpan(operationName, opts...)
}

// setSamplingPriorityWhileLocked updates the sampling priority.
// Note: This method assumes the span lock is held but releases it to avoid deadlock
// when updating the trace's sampling priority.
// +checklocks:s.mu
func (s *Span) setSamplingPriorityWhileLocked(priority int, sampler samplernames.SamplerName) {
	assert.MutexLocked(&s.mu)

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		return
	}
	s.setMetricWhileLocked(keySamplingPriority, float64(priority))
	context := s.context // get context reference while holding lock
	s.mu.Unlock()

	// Call trace's setSamplingPriority outside span lock to avoid deadlock
	context.setSamplingPriority(priority, sampler)

	// Re-acquire lock to maintain the contract of this method
	s.mu.Lock()
}

// setTagError sets the error tag. It accounts for various valid scenarios.
// This method is not safe for concurrent use.
func (s *Span) setTagError(value interface{}, cfg errorConfig) {
	s.mu.Lock()
	s.setTagErrorWhileLocked(value, cfg)
	s.mu.Unlock()
}

// +checklocks:s.mu
func (s *Span) setTagErrorWhileLocked(value interface{}, cfg errorConfig) {
	assert.MutexLocked(&s.mu)

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		return
	}

	switch v := value.(type) {
	case bool:
		// bool value as per Opentracing spec.
		s.setErrorWhileLocked(v)
	case error:
		// if anyone sets an error value as the tag, be nice here
		// and provide all the benefits.
		s.setErrorWhileLocked(true)
		s.setMetaWhileLocked(ext.ErrorMsg, v.Error())
		s.setMetaWhileLocked(ext.ErrorType, reflect.TypeOf(v).String())
		switch err := v.(type) {
		case xerrors.Formatter:
			s.setMetaWhileLocked(ext.ErrorDetails, fmt.Sprintf("%+v", v))
		case fmt.Formatter:
			// pkg/errors approach
			s.setMetaWhileLocked(ext.ErrorDetails, fmt.Sprintf("%+v", v))
		case *errortrace.TracerError:
			// instrumentation/errortrace approach
			s.setMetaWhileLocked(ext.ErrorDetails, fmt.Sprintf("%+v", v))
			if !cfg.noDebugStack {
				s.setMetaWhileLocked(ext.ErrorStack, err.Format())
			}
			return
		}
		if !cfg.noDebugStack {
			s.setMetaWhileLocked(ext.ErrorStack, takeStacktrace(cfg.stackFrames, cfg.stackSkip))
		}
	case nil:
		// no error
		s.setErrorWhileLocked(false)
	default:
		// in all other cases, let's assume that setting this tag
		// is the result of an error.
		s.setErrorWhileLocked(true)
	}
}

// +checklocks:s.mu
func (s *Span) setErrorWhileLocked(yes bool) {
	assert.MutexLocked(&s.mu)

	if yes {
		if s.errorStatus == 0 {
			// new error
			s.context.errors.Add(1)
		}
		s.errorStatus = 1
		return
	}

	if s.errorStatus > 0 {
		// flip from active to inactive
		s.context.errors.Add(-1)
	}
	s.errorStatus = 0
}

// defaultStackLength specifies the default maximum size of a stack trace.
const defaultStackLength = 32

// takeStacktrace takes a stack trace of maximum n entries, skipping the first skip entries.
// If n is 0, up to 20 entries are retrieved.
func takeStacktrace(n, skip uint) string {
	telemetry.Count(telemetry.NamespaceTracers, "errorstack.source", []string{"source:takeStacktrace"}).Submit(1)
	now := time.Now()
	defer func() {
		dur := float64(time.Since(now))
		telemetry.Distribution(telemetry.NamespaceTracers, "errorstack.duration", []string{"source:takeStacktrace"}).Submit(dur)
	}()
	if n == 0 {
		n = defaultStackLength
	}
	var builder strings.Builder
	pcs := make([]uintptr, n)

	// +2 to exclude runtime.Callers and takeStacktrace
	numFrames := runtime.Callers(2+int(skip), pcs)
	if numFrames == 0 {
		return ""
	}
	frames := runtime.CallersFrames(pcs[:numFrames])
	for i := 0; ; i++ {
		frame, more := frames.Next()
		if i != 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(frame.Function)
		builder.WriteByte('\n')
		builder.WriteByte('\t')
		builder.WriteString(frame.File)
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(frame.Line))
		if !more {
			break
		}
	}
	return builder.String()
}

// setMetaWhileLocked sets a string tag. This method is not safe for concurrent use.
// +checklocks:s.mu
func (s *Span) setMetaWhileLocked(key, v string) {
	assert.MutexLocked(&s.mu)

	if s.meta == nil {
		s.meta = make(map[string]string, 1)
	}
	delete(s.metrics, key)
	switch key {
	case ext.SpanName:
		s.name = v
	case ext.ServiceName:
		s.service = v
	case ext.ResourceName:
		s.resource = v
	case ext.SpanType:
		s.spanType = v
	default:
		s.meta[key] = v
	}
}

func (s *Span) setMetadatum(key, v string) {
	s.mu.Lock()
	s.meta[key] = v
	s.mu.Unlock()
}

func (s *Span) getMetadatum(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.meta[key]
	return val, ok
}

func (s *Span) fetchMetadatum(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta[key]
}

func (s *Span) getMetadata() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.meta)
}

func (s *Span) getMetaStruct() metaStructMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metaStruct
}

// +checklocks:s.mu
func (s *Span) setMetaStructWhileLocked(key string, v any) {
	assert.MutexLocked(&s.mu)

	if s.metaStruct == nil {
		s.metaStruct = make(metaStructMap, 1)
	}
	s.metaStruct[key] = v
}

// setTagBoolWhileLocked sets a boolean tag on the span.
// +checklocks:s.mu
func (s *Span) setTagBoolWhileLocked(key string, v bool) {
	assert.MutexLocked(&s.mu)

	switch key {
	case ext.AnalyticsEvent:
		if v {
			s.setMetricWhileLocked(ext.EventSampleRate, 1.0)
		} else {
			s.setMetricWhileLocked(ext.EventSampleRate, 0.0)
		}
	case ext.ManualDrop:
		if v {
			s.setSamplingPriorityWhileLocked(ext.PriorityUserReject, samplernames.Manual)
		}
	case ext.ManualKeep:
		if v {
			s.setSamplingPriorityWhileLocked(ext.PriorityUserKeep, samplernames.Manual)
		}
	default:
		if v {
			s.setMetaWhileLocked(key, "true")
		} else {
			s.setMetaWhileLocked(key, "false")
		}
	}
}

func (s *Span) setMetric(key string, v float64) {
	s.mu.Lock()
	s.metrics[key] = v
	s.mu.Unlock()
}

func (s *Span) deleteMetric(key string) {
	s.mu.Lock()
	delete(s.metrics, key)
	s.mu.Unlock()
}

func (s *Span) getMetric(key string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.metrics[key]
	return val, ok
}

// TODO(kakkoyun): How to eliminate this usage?
// used in internal/civisibility/integrations/manual_api_common.go using linkname
func getMetric(s *Span, key string) (float64, bool) {
	return s.getMetric(key)
}

func (s *Span) fetchMetric(key string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metrics[key]
}

func (s *Span) getMetrics() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.metrics)
}

// setMetricWhileLocked sets a numeric tag, in our case called a metric.
// This method is not safe for concurrent use.
// +checklocks:s.mu
func (s *Span) setMetricWhileLocked(key string, v float64) {
	assert.MutexLocked(&s.mu)

	if s.metrics == nil {
		s.metrics = make(map[string]float64, 1)
	}
	delete(s.meta, key)
	switch key {
	case ext.ManualKeep:
		if v == float64(samplernames.AppSec) {
			s.setSamplingPriorityWhileLocked(ext.PriorityUserKeep, samplernames.AppSec)
		}
	case "_sampling_priority_v1shim":
		// We have this for backward compatibility with the v1 shim.
		s.setSamplingPriorityWhileLocked(int(v), samplernames.Manual)
	default:
		s.metrics[key] = v
	}
}

// AddLink appends the given link to the span's span links.
func (s *Span) AddLink(link SpanLink) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		// already finished
		return
	}
	s.spanLinks = append(s.spanLinks, link)
}

func (s *Span) getSpanLinks() []SpanLink {
	s.mu.Lock()
	defer s.mu.Unlock()
	links := make([]SpanLink, len(s.spanLinks))
	copy(links, s.spanLinks)
	return links
}

// AddEvent attaches a new event to the current span.
func (s *Span) AddEvent(name string, opts ...SpanEventOption) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		return
	}
	cfg := SpanEventConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Time.IsZero() {
		cfg.Time = time.Now()
	}
	event := spanEvent{
		Name:         name,
		TimeUnixNano: uint64(cfg.Time.UnixNano()),
	}
	if s.supportsEvents {
		event.Attributes = toSpanEventAttributeMsg(cfg.Attributes)
	} else {
		event.RawAttributes = cfg.Attributes
	}
	s.spanEvents = append(s.spanEvents, event)
}

func (s *Span) getSpanEvents() []spanEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]spanEvent, len(s.spanEvents))
	copy(events, s.spanEvents)
	return events
}

// SetOperationName sets or changes the operation name.
func (s *Span) SetOperationName(operationName string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		// already finished
		return
	}
	s.name = operationName
}

func (s *Span) getErrorStatus() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errorStatus
}

func (s *Span) getActivePprofContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pprofCtxActive
}

func (s *Span) setActivePprofContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pprofCtxActive = ctx
}

func (s *Span) saveRestorePprofContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pprofCtxRestore = ctx
}

// serializeSpanLinksInMetaWhileLocked saves span links as a JSON string under `Span[meta][_dd.span_links]`.
// +checklocks:s.mu
func (s *Span) serializeSpanLinksInMetaWhileLocked() {
	assert.MutexLocked(&s.mu)

	if len(s.spanLinks) == 0 {
		return
	}
	spanLinkBytes, err := json.Marshal(s.spanLinks)
	if err != nil {
		log.Debug("Unable to marshal span links. Not adding span links to span meta.")
		return
	}
	if s.meta == nil {
		s.meta = make(map[string]string)
	}
	s.meta["_dd.span_links"] = string(spanLinkBytes)
}

// serializeSpanEventsWhileLocked sets the span events from the current span in the correct transport, depending on whether the
// agent supports the native method or not.
// +checklocks:s.mu
func (s *Span) serializeSpanEventsWhileLocked() {
	assert.MutexLocked(&s.mu)

	if len(s.spanEvents) == 0 {
		return
	}
	// if span events are natively supported by the agent, there's nothing to do
	// as the events will be already included when the span is serialized.
	if s.supportsEvents {
		return
	}
	// otherwise, we need to serialize them as a string tag and remove them from the struct
	// so they are not sent twice.
	b, err := json.Marshal(s.spanEvents)
	s.spanEvents = nil
	if err != nil {
		log.Debug("Unable to marshal span events; events dropped from span meta\n%s", err.Error())
		return
	}
	s.meta["events"] = string(b)
}

func (s *Span) markFinished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	s.finished = true
}

// Finish closes this Span (but not its children) providing the duration
// of its part of the tracing session.
func (s *Span) Finish(opts ...FinishOption) {
	if s == nil {
		return
	}

	if s.isRoot() {
		if tr, ok := getGlobalTracer().(*tracer); ok && tr.rulesSampler.traces.enabled() {
			spanCxt := s.Context()
			spanCxt.trace.attemptRulesSampling(tr.rulesSampler)
		}
	}

	s.mu.Lock()

	t := now()
	if len(opts) > 0 {
		cfg := FinishConfig{
			NoDebugStack: s.noDebugStack,
		}
		for _, fn := range opts {
			if fn == nil {
				continue
			}
			fn(&cfg)
		}
		if !cfg.FinishTime.IsZero() {
			t = cfg.FinishTime.UnixNano()
		}
		if cfg.Error != nil {
			s.setTagErrorWhileLocked(cfg.Error, errorConfig{
				noDebugStack: cfg.NoDebugStack,
				stackFrames:  cfg.StackFrames,
				stackSkip:    cfg.SkipStackFrames,
			})
		}
	}

	if s.goExecTraced && rt.IsEnabled() {
		// Only tag spans as traced if they both started & ended with
		// execution tracing enabled. This is technically not sufficient
		// for spans which could straddle the boundary between two
		// execution traces, but there's really nothing we can do in
		// those cases since execution tracing tasks aren't recorded in
		// traces if they started before the trace.
		s.setTagWhileLocked("go_execution_traced", "yes")
	} else if s.goExecTraced {
		// If the span started with tracing enabled, but tracing wasn't
		// enabled when the span finished, we still have some data to
		// show. If tracing wasn't enabled when the span started, we
		// won't have data in the execution trace to identify it so
		// there's nothign we can show.
		s.setTagWhileLocked("go_execution_traced", "partial")
	}

	// We don't lock spans when flushing, so we could have a data race when
	// modifying a span as it's being flushed. This protects us against that
	// race, since spans are marked `finished` before we flush them.
	if s.finished {
		// already finished
		s.mu.Unlock()
		return
	}

	s.finishWhileLocked(t)
	s.mu.Unlock()

	// Call context.finish() after releasing the lock to avoid deadlocks
	// in onSpanFinished which needs to acquire the span lock
	s.Context().finish()

	if log.DebugEnabled() {
		log.Debug("Finished Span: %v", s.snapshot()) //nolint:gocritic // snapshot is safe to print.
	}
	orchestrion.GLSPopValue(sharedinternal.ActiveSpanKey)
}

// TODO(kakkoyun): If it is finished, let the rest of the calls no-op.
func (s *Span) isFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

// isFinishedWhileLocked returns whether the span is finished.
// This method assumes that the trace lock is held, avoiding the deadlock
// that would occur if we tried to acquire the span lock while holding the trace lock.
// +checklocks:s.mu
func (s *Span) isFinishedWhileLocked() bool {
	// The finished field can be read when the trace is locked per the comment in the struct
	return s.finished
}

// +checklocks:s.mu
func (s *Span) finishWhileLocked(finishTime int64) {
	assert.MutexLocked(&s.mu)

	if s.duration == 0 {
		s.duration = finishTime - s.start
	}
	if s.duration < 0 {
		s.duration = 0
	}
	if s.taskEnd != nil {
		s.taskEnd()
	}

	keep := true
	tracer, isTracer := getGlobalTracer().(*tracer)
	if !isTracer {
		return
	}

	if !tracer.config.enabled.get() {
		return
	}
	if tracer.config.canDropP0s() {
		// the agent supports dropping p0's in the client
		keep = s.shouldKeepWhileLocked()
	}
	if tracer.config.debugAbandonedSpans {
		// the tracer supports debugging abandoned spans.
		tracer.submitAbandonedSpan(s.snapshotWhileLocked(), true)
	}
	tracer.spansFinished.Inc(s.integration)

	if keep {
		// a single kept span keeps the whole trace.
		s.context.trace.keep()
	}
	// compute stats after finishing the span.
	// This ensures any normalization or tag propagation has been applied
	// TODO(kakkoyun): Check if the above comment is up-to-date.
	s.serializeSpanLinksInMetaWhileLocked()
	s.serializeSpanEventsWhileLocked()
	tracer.submit(s.snapshotWhileLocked())

	if s.pprofCtxRestore != nil {
		// Restore the labels of the parent span so any CPU samples after this
		// point are attributed correctly.
		pprof.SetGoroutineLabels(s.pprofCtxRestore)
	}
}

// textNonParsable specifies the text that will be assigned to resources for which the resource
// can not be parsed due to an obfuscation error.
const textNonParsable = "Non-parsable SQL query"

// obfuscatedResource returns the obfuscated version of the given resource. It is
// obfuscated using the given obfuscator for the given span type typ.
func obfuscatedResource(o *obfuscate.Obfuscator, typ, resource string) string {
	if o == nil {
		return resource
	}
	switch typ {
	case "sql", "cassandra":
		oq, err := o.ObfuscateSQLString(resource)
		if err != nil {
			log.Error("Error obfuscating stats group resource %q: %v", resource, err.Error())
			return textNonParsable
		}
		return oq.Query
	case "redis":
		return o.QuantizeRedisString(resource)
	default:
		return resource
	}
}

func (s *Span) shouldKeep() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shouldKeepWhileLocked()
}

// shouldKeepWhileLocked reports whether the trace should be kept.
// a single span being kept implies the whole trace being kept.
// +checklocks:s.mu
func (s *Span) shouldKeepWhileLocked() bool {
	assert.MutexLocked(&s.mu)

	if p, ok := s.context.SamplingPriority(); ok && p > 0 {
		// positive sampling priorities stay
		return true
	}
	if s.context.errors.Load() > 0 {
		// traces with any span containing an error get kept
		return true
	}
	if v, ok := s.metrics[ext.EventSampleRate]; ok {
		return sampledByRate(s.traceID, v)
	}
	return false
}

func (s *Span) shouldComputeStats() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shouldComputeStatsWhileLocked()
}

// shouldComputeStatsWhileLocked mentions whether this span needs to have stats computed for.
// +checklocks:s.mu
func (s *Span) shouldComputeStatsWhileLocked() bool {
	assert.MutexLocked(&s.mu)

	if v, ok := s.metrics[keyMeasured]; ok && v == 1 {
		return true
	}
	if v, ok := s.metrics[keyTopLevel]; ok && v == 1 {
		return true
	}
	return false
}

// String returns a human readable representation of the span. Not for
// production, just debugging.
func (s *Span) String() string {
	if s == nil {
		return "<nil>"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stringWhileLocked()
}

// +checklocks:s.mu
func (s *Span) stringWhileLocked() string {
	assert.MutexLocked(&s.mu)

	lines := []string{
		fmt.Sprintf("Name: %s", s.name),
		fmt.Sprintf("Service: %s", s.service),
		fmt.Sprintf("Resource: %s", s.resource),
		fmt.Sprintf("TraceID: %d", s.traceID),
		fmt.Sprintf("TraceID128: %s", s.context.TraceID()),
		fmt.Sprintf("SpanID: %d", s.spanID),
		fmt.Sprintf("ParentID: %d", s.parentID),
		fmt.Sprintf("Start: %s", time.Unix(0, s.start)),
		fmt.Sprintf("Duration: %s", time.Duration(s.duration)),
		fmt.Sprintf("Error: %d", s.errorStatus),
		fmt.Sprintf("Type: %s", s.spanType),
		"Tags:",
	}
	for key, val := range s.meta {
		lines = append(lines, fmt.Sprintf("\t%s:%s", key, val))
	}
	for key, val := range s.metrics {
		lines = append(lines, fmt.Sprintf("\t%s:%f", key, val))
	}
	return strings.Join(lines, "\n")
}

// Format implements fmt.Formatter.
func (s *Span) Format(f fmt.State, c rune) {
	if s == nil {
		fmt.Fprintf(f, "<nil>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c {
	case 's':
		fmt.Fprint(f, s.stringWhileLocked())
	case 'v':
		s.formatWhileLocked(f)
	default:
		fmt.Fprintf(f, "%%!%c(tracer.Span=", c)
		s.formatWhileLocked(f)
		fmt.Fprintf(f, ")")
	}
}

// +checklocks:s.mu
func (s *Span) formatWhileLocked(f fmt.State) {
	assert.MutexLocked(&s.mu)

	if svc := globalconfig.ServiceName(); svc != "" {
		fmt.Fprintf(f, "dd.service=%s ", svc)
	}
	if tr := getGlobalTracer(); tr != nil {
		tc := tr.TracerConf()
		if tc.EnvTag != "" {
			fmt.Fprintf(f, "dd.env=%s ", tc.EnvTag)
		} else if env := os.Getenv("DD_ENV"); env != "" {
			fmt.Fprintf(f, "dd.env=%s ", env)
		}
		if tc.VersionTag != "" {
			fmt.Fprintf(f, "dd.version=%s ", tc.VersionTag)
		} else if v := os.Getenv("DD_VERSION"); v != "" {
			fmt.Fprintf(f, "dd.version=%s ", v)
		}
	}
	var traceID string
	if sharedinternal.BoolEnv("DD_TRACE_128_BIT_TRACEID_LOGGING_ENABLED", true) && s.context.traceID.HasUpper() {
		traceID = s.context.TraceID()
	} else {
		traceID = fmt.Sprintf("%d", s.traceID)
	}
	fmt.Fprintf(f, `dd.trace_id=%q `, traceID)
	fmt.Fprintf(f, `dd.span_id="%d" `, s.spanID)
	fmt.Fprintf(f, `dd.parent_id="%d"`, s.parentID)
}

// used in internal/civisibility/integrations/manual_api_common.go using linkname
func getMetadatum(s *Span, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.meta[key]
	return val, ok
}

// extractMeta retrieves a metadata value from a span and removes it from the span's metadata and metrics.
func (s *Span) fetchAndDeleteMetadatum(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meta == nil {
		// TODO(kakkoyun): testing: Check if this change anything.
		// span.meta = make(map[string]string, 1)
		// Fast-path: if the span has no metadata, return an empty string.
		return ""
	}

	if v, ok := s.meta[key]; ok {
		delete(s.meta, key)
		delete(s.metrics, key)
		return v
	}
	return ""
}

// setPeerServiceWhileLocked sets the peer.service, _dd.peer.service.source, and _dd.peer.service.remapped_from
// tags as applicable for the given span.
func (s *Span) setPeerService(peerServiceDefaults bool, peerServiceMappings map[string]string) {
	s.mu.Lock()
	s.setPeerServiceWhileLocked(peerServiceDefaults, peerServiceMappings)
	s.mu.Unlock()
}

// +checklocks:s.mu
func (s *Span) setPeerServiceWhileLocked(peerServiceDefaults bool, peerServiceMappings map[string]string) {
	assert.MutexLocked(&s.mu)

	if _, ok := s.meta[ext.PeerService]; ok { // peer.service already set on the span
		s.setMetaWhileLocked(keyPeerServiceSource, ext.PeerService)
	} else { // no peer.service currently set
		spanKind := s.meta[ext.SpanKind]
		isOutboundRequest := spanKind == ext.SpanKindClient || spanKind == ext.SpanKindProducer
		shouldSetDefaultPeerService := isOutboundRequest && peerServiceDefaults
		if !shouldSetDefaultPeerService {
			return
		}
		source := s.setPeerServiceFromSourceWhileLocked()
		if source == "" {
			log.Debug("No source tag value could be found for span %q, peer.service not set", s.name)
			return
		}
		s.setMetaWhileLocked(keyPeerServiceSource, source)
	}
	// Overwrite existing peer.service value if remapped by the user
	ps := s.meta[ext.PeerService]
	if to, ok := peerServiceMappings[ps]; ok {
		s.setMetaWhileLocked(keyPeerServiceRemappedFrom, ps)
		s.setMetaWhileLocked(ext.PeerService, to)
	}
}

// setPeerServiceFromSourceWhileLocked sets peer.service from the sources determined
// by the tags on the span. It returns the source tag name that it used for
// the peer.service value, or the empty string if no valid source tag was available.
// +checklocks:s.mu
func (s *Span) setPeerServiceFromSourceWhileLocked() string {
	assert.MutexLocked(&s.mu)

	has := func(tag string) bool {
		_, ok := s.meta[tag]
		return ok
	}
	var sources []string
	useTargetHost := true
	switch {
	// order of the cases and their sources matters here. These are in priority order (highest to lowest)
	case has("aws_service"):
		sources = []string{
			"queuename",
			"topicname",
			"streamname",
			"tablename",
			"bucketname",
		}
	case s.meta[ext.DBSystem] == ext.DBSystemCassandra:
		sources = []string{
			ext.CassandraContactPoints,
		}
		useTargetHost = false
	case has(ext.DBSystem):
		sources = []string{
			ext.DBName,
			ext.DBInstance,
		}
	case has(ext.MessagingSystem):
		sources = []string{
			ext.KafkaBootstrapServers,
		}
	case has(ext.RPCSystem):
		sources = []string{
			ext.RPCService,
		}
	}
	// network destination tags will be used as fallback unless there are higher priority sources already set.
	if useTargetHost {
		sources = append(sources, []string{
			ext.NetworkDestinationName,
			ext.PeerHostname,
			ext.TargetHost,
		}...)
	}
	for _, source := range sources {
		if val, ok := s.meta[source]; ok {
			s.setMetaWhileLocked(ext.PeerService, val)
			return source
		}
	}
	return ""
}

func (s *Span) isResourcePIISafe() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isResourcePIISafeWhileLocked()
}

// isResourcePIISafeWhileLocked returns true if s.resource can be considered to not
// include PII with reasonable confidence. E.g. SQL queries may contain PII,
// but http, rpc or custom (s.spanType == "") span resource names generally do not.
// +checklocks:s.mu
func (s *Span) isResourcePIISafeWhileLocked() bool {
	assert.MutexLocked(&s.mu)
	return s.spanType == ext.SpanTypeWeb || s.spanType == ext.AppTypeRPC || s.spanType == ""
}

const (
	keySamplingPriority     = "_sampling_priority_v1"
	keySamplingPriorityRate = "_dd.agent_psr"
	keyDecisionMaker        = "_dd.p.dm"
	keyServiceHash          = "_dd.dm.service_hash"
	keyOrigin               = "_dd.origin"
	keyReparentID           = "_dd.parent_id"
	// keyHostname can be used to override the agent's hostname detection when using `WithHostname`.
	// which is set via auto-detection.
	keyHostname                = "_dd.hostname"
	keyRulesSamplerAppliedRate = "_dd.rule_psr"
	keyRulesSamplerLimiterRate = "_dd.limit_psr"
	keyMeasured                = "_dd.measured"
	// keyTopLevel is the key of top level metric indicating if a span is top level.
	// A top level span is a local root (parent span of the local trace) or the first span of each service.
	keyTopLevel = "_dd.top_level"
	// keyPropagationError holds any error from propagated trace tags (if any)
	keyPropagationError = "_dd.propagation_error"
	// keySpanSamplingMechanism specifies the sampling mechanism by which an individual span was sampled
	keySpanSamplingMechanism = "_dd.span_sampling.mechanism"
	// keySingleSpanSamplingRuleRate specifies the configured sampling probability for the single span sampling rule.
	keySingleSpanSamplingRuleRate = "_dd.span_sampling.rule_rate"
	// keySingleSpanSamplingMPS specifies the configured limit for the single span sampling rule
	// that the span matched. If there is no configured limit, then this tag is omitted.
	keySingleSpanSamplingMPS = "_dd.span_sampling.max_per_second"
	// keyPropagatedUserID holds the propagated user identifier, if user id propagation is enabled.
	keyPropagatedUserID = "_dd.p.usr.id"
	// keyPropagatedTraceSource holds a 2 character hexadecimal string representation of the product responsible
	// for the span creation.
	keyPropagatedTraceSource = "_dd.p.ts"
	// keyTraceID128 is the lowercase, hex encoded upper 64 bits of a 128-bit trace id, if present.
	keyTraceID128 = "_dd.p.tid"
	// keySpanAttributeSchemaVersion holds the selected DD_TRACE_SPAN_ATTRIBUTE_SCHEMA version.
	keySpanAttributeSchemaVersion = "_dd.trace_span_attribute_schema"
	// keyPeerServiceSource indicates the precursor tag that was used as the value of peer.service.
	keyPeerServiceSource = "_dd.peer.service.source"
	// keyPeerServiceRemappedFrom indicates the previous value for peer.service, in case remapping happened.
	keyPeerServiceRemappedFrom = "_dd.peer.service.remapped_from"
	// keyBaseService contains the globally configured tracer service name. It is only set for spans that override it.
	keyBaseService = "_dd.base_service"
	// keyProcessTags contains a list of process tags to indentify the service.
	keyProcessTags = "_dd.tags.process"
)

// The following set of tags is used for user monitoring and set through calls to span.SetUser().
const (
	keyUserID        = "usr.id"
	keyUserLogin     = "usr.login"
	keyUserEmail     = "usr.email"
	keyUserName      = "usr.name"
	keyUserOrg       = "usr.org"
	keyUserRole      = "usr.role"
	keyUserScope     = "usr.scope"
	keyUserSessionID = "usr.session_id"
)
