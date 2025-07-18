// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
	"github.com/stretchr/testify/assert"
)

// Tests to confirm that when the payload queue is full, chunks are dropped
// and the associated trace is counted as dropped.
func TestTraceFinishChunk(t *testing.T) {
	assert := assert.New(t)
	tracer, err := newUnstartedTracer()
	assert.Nil(err)
	defer tracer.statsd.Close()

	root := newSpan("name", "service", "resource", 0, 0, 0)
	trace := root.Context().trace

	for i := 0; i < payloadQueueSize+1; i++ {
		trace.mu.Lock()
		c := chunk{spans: make([]recordingSpan, 1)}
		trace.finishChunkWhileLocked(tracer, &c)
		trace.mu.Unlock()
	}
	assert.Equal(uint32(1), atomic.LoadUint32((*uint32)(&tracer.totalTracesDropped)))
}

func TestSpanTracePushNoFinish(t *testing.T) {
	defer setupteardown(2, 5)()

	assert := assert.New(t)

	tp := new(log.RecordLogger)
	tp.Ignore("appsec: ", "telemetry")
	_, _, _, stop, err := startTestTracer(t, WithLogger(tp), WithLambdaMode(true), WithEnv("testEnv"))
	assert.NoError(err)
	defer stop()

	buffer := newTrace()
	assert.NotNil(buffer)
	assert.Len(buffer.getSpans(), 0)

	traceID := randUint64()
	root := newSpan("name1", "a-service", "a-resource", traceID, traceID, 0)
	root.Context().trace = buffer

	buffer.push(root)
	spans := buffer.getSpans()
	assert.Len(spans, 1, "there is one span in the buffer")
	assert.Equal(root, spans[0], "the span is the one pushed before")

	<-time.After(time.Second / 10)
	log.Flush()
	assert.Len(tp.Logs(), 0)
	t.Logf("expected timeout, nothing should show up in buffer as the trace is not finished")
}

func TestSpanTracePushSeveral(t *testing.T) {
	defer setupteardown(2, 5)()

	assert := assert.New(t)

	trc, transport, flush, stop, err := startTestTracer(t)
	assert.Nil(err)
	defer stop()
	buffer := newTrace()
	assert.NotNil(buffer)
	assert.Len(buffer.getSpans(), 0)

	traceID := randUint64()
	root := trc.StartSpan("name1", WithSpanID(traceID))
	span2 := trc.StartSpan("name2", ChildOf(root.Context()))
	span3 := trc.StartSpan("name3", ChildOf(root.Context()))
	span3a := trc.StartSpan("name3", ChildOf(span3.Context()))

	trace := []recordingSpan{root, span2, span3, span3a}

	for i, span := range trace {
		span.Context().trace = buffer
		buffer.push(span)
		spans := buffer.getSpans()
		assert.Len(spans, i+1, "there is one more span in the buffer")
		assert.Equal(span, spans[i], "the span is the one pushed before")
	}

	for _, span := range trace {
		span.Finish()
	}
	flush(1)

	traces := transport.Traces()
	assert.Len(traces, 1)
	frozenTrace := traces[0]
	assert.Len(frozenTrace, 4, "there was one trace with the right number of spans in the channel")
	for _, span := range frozenTrace {
		assert.Contains(frozenTrace, span, "the trace contains the spans")
	}
}

func TestSetSamplingPriorityLocked(t *testing.T) {
	t.Run("NoPriorAndP0IsIgnored", func(t *testing.T) {
		tr := trace{
			propagatingTags: map[string]string{},
		}
		tr.setSamplingPriority(ext.PriorityAutoReject, samplernames.RemoteRate)
		assert.Empty(t, tr.propagatingTags[keyDecisionMaker])
	})
	t.Run("UnknownSamplerIsIgnored", func(t *testing.T) {
		tr := trace{
			propagatingTags: map[string]string{},
		}
		tr.setSamplingPriority(ext.PriorityAutoReject, samplernames.Unknown)
		assert.Empty(t, tr.propagatingTags[keyDecisionMaker])
	})
	t.Run("NoPriorAndP1IsAccepted", func(t *testing.T) {
		tr := trace{
			propagatingTags: map[string]string{},
		}
		tr.setSamplingPriority(ext.PriorityAutoKeep, samplernames.RemoteRate)
		assert.Equal(t, "-2", tr.propagatingTags[keyDecisionMaker])
	})
	t.Run("PriorAndP1AndSameDMIsIgnored", func(t *testing.T) {
		tr := trace{
			propagatingTags: map[string]string{keyDecisionMaker: "-1"},
		}
		tr.setSamplingPriority(ext.PriorityAutoKeep, samplernames.AgentRate)
		assert.Equal(t, "-1", tr.propagatingTags[keyDecisionMaker])
	})
	t.Run("PriorAndP1DifferentDMAccepted", func(t *testing.T) {
		tr := trace{
			propagatingTags: map[string]string{keyDecisionMaker: "-1"},
		}
		tr.setSamplingPriority(ext.PriorityAutoKeep, samplernames.RemoteRate)
		assert.Equal(t, "-2", tr.propagatingTags[keyDecisionMaker])
	})
}
