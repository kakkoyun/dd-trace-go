// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import "testing"

func TestSpanSnapshotEquality(t *testing.T) {
	span := newBasicSpan("op")
	span.SetTag("k", "v")
	span.AddEvent("evt")
	span.AddLink(SpanLink{TraceID: 1, SpanID: 2})

	snap := span.snapshot()

	if snap.name != span.getName() || snap.service != span.getService() || snap.resource != span.getResource() {
		t.Fatalf("snapshot fields mismatch")
	}
	if got := snap.metas["k"]; got != "v" {
		t.Fatalf("meta mismatch: %s", got)
	}
	if len(snap.spanLinks) != 1 || snap.spanLinks[0].SpanID != 2 {
		t.Fatalf("span links not copied")
	}
}
