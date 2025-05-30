// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package redis

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/DataDog/dd-trace-go/v2/instrumentation/testutils"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

const debug = false

func TestMain(m *testing.M) {
	_, ok := os.LookupEnv("INTEGRATION")
	if !ok {
		fmt.Println("--- SKIP: to enable integration test, set the INTEGRATION environment variable")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestClientEvalSha(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client := NewClient(opts, WithService("my-redis"))

	sha1 := client.ScriptLoad("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}").Val()
	mt.Reset()

	client.EvalSha(sha1, []string{"key1", "key2", "first", "second"})

	spans := mt.FinishedSpans()
	assert.Len(spans, 1)

	span := spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
	assert.Equal("6379", span.Tag(ext.TargetPort))
	assert.Equal("evalsha", span.Tag(ext.ResourceName))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("0", span.Tag("out.db"))
	assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))
}

// https://github.com/DataDog/dd-trace-go/issues/387
func TestIssue387(_ *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	client := NewClient(opts, WithService("my-redis"))
	n := 1000

	client.Set("test_key", "test_value", 0)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			client.WithContext(context.Background()).Get("test_key").Result()
		}()
	}
	wg.Wait()

	// should not result in a race
}

func TestClient(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379", DB: 15}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client := NewClient(opts, WithService("my-redis"))
	client.Set("test_key", "test_value", 0)

	spans := mt.FinishedSpans()
	assert.Len(spans, 1)

	span := spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
	assert.Equal("6379", span.Tag(ext.TargetPort))
	assert.Equal("set test_key test_value: ", span.Tag("redis.raw_command"))
	assert.Equal("3", span.Tag("redis.args_length"))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("15", span.Tag("out.db"))
	assert.Equal(float64(15), span.Tag(ext.RedisDatabaseIndex))
}

func TestPipeline(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client := NewClient(opts, WithService("my-redis"))
	pipeline := client.Pipeline()
	pipeline.Expire("pipeline_counter", time.Hour)

	// Exec with context test
	pipeline.(*Pipeliner).ExecWithContext(context.Background())

	spans := mt.FinishedSpans()
	assert.Len(spans, 1)

	span := spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("expire pipeline_counter 3600: false\n", span.Tag(ext.ResourceName))
	assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
	assert.Equal("6379", span.Tag(ext.TargetPort))
	assert.Equal("1", span.Tag("redis.pipeline_length"))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("0", span.Tag("out.db"))
	assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))

	mt.Reset()
	pipeline.Expire("pipeline_counter", time.Hour)
	pipeline.Expire("pipeline_counter_1", time.Minute)

	// Rewriting Exec
	pipeline.Exec()

	spans = mt.FinishedSpans()
	assert.Len(spans, 1)

	span = spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("expire pipeline_counter 3600: false\nexpire pipeline_counter_1 60: false\n", span.Tag(ext.ResourceName))
	assert.Equal("2", span.Tag("redis.pipeline_length"))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("0", span.Tag("out.db"))
	assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))
}

func TestPipelined(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client := NewClient(opts, WithService("my-redis"))
	_, err := client.Pipelined(func(p redis.Pipeliner) error {
		p.Expire("pipeline_counter", time.Hour)
		return nil
	})
	assert.NoError(err)

	spans := mt.FinishedSpans()
	assert.Len(spans, 1)

	span := spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("expire pipeline_counter 3600: false\n", span.Tag(ext.ResourceName))
	assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
	assert.Equal("6379", span.Tag(ext.TargetPort))
	assert.Equal("1", span.Tag("redis.pipeline_length"))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("0", span.Tag("out.db"))
	assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))

	mt.Reset()
	_, err = client.Pipelined(func(p redis.Pipeliner) error {
		p.Expire("pipeline_counter", time.Hour)
		p.Expire("pipeline_counter_1", time.Minute)
		return nil
	})
	assert.NoError(err)

	spans = mt.FinishedSpans()
	assert.Len(spans, 1)

	span = spans[0]
	assert.Equal("redis.command", span.OperationName())
	assert.Equal(ext.SpanTypeRedis, span.Tag(ext.SpanType))
	assert.Equal("my-redis", span.Tag(ext.ServiceName))
	assert.Equal("expire pipeline_counter 3600: false\nexpire pipeline_counter_1 60: false\n", span.Tag(ext.ResourceName))
	assert.Equal("2", span.Tag("redis.pipeline_length"))
	assert.Equal("go-redis/redis", span.Tag(ext.Component))
	assert.Equal(componentName, span.Integration())
	assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
	assert.Equal("redis", span.Tag(ext.DBSystem))
	assert.Equal("0", span.Tag("out.db"))
	assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))
}

func TestChildSpan(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	// Parent span
	client := NewClient(opts, WithService("my-redis"))
	root, ctx := tracer.StartSpanFromContext(context.Background(), "parent.span")
	client = client.WithContext(ctx)
	client.Set("test_key", "test_value", 0)
	root.Finish()

	spans := mt.FinishedSpans()
	assert.Len(spans, 2)

	var child, parent *mocktracer.Span
	for _, s := range spans {
		// order of traces in buffer is not garanteed
		switch s.OperationName() {
		case "redis.command":
			child = s
		case "parent.span":
			parent = s
		}
	}
	assert.NotNil(parent)
	assert.NotNil(child)

	assert.Equal(child.ParentID(), parent.SpanID())
	assert.Equal(child.Tag(ext.TargetHost), "127.0.0.1")
	assert.Equal(child.Tag(ext.TargetPort), "6379")
}

func TestMultipleCommands(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client := NewClient(opts, WithService("my-redis"))
	client.Set("test_key", "test_value", 0)
	client.Get("test_key")
	client.Incr("int_key")
	client.ClientList()

	spans := mt.FinishedSpans()
	assert.Len(spans, 4)

	// Checking all commands were recorded
	var commands [4]string
	for i := 0; i < 4; i++ {
		commands[i] = spans[i].Tag("redis.raw_command").(string)
	}
	assert.Contains(commands, "set test_key test_value: ")
	assert.Contains(commands, "get test_key: ")
	assert.Contains(commands, "incr int_key: 0")
	assert.Contains(commands, "client list: ")
}

func TestError(t *testing.T) {
	t.Run("wrong-port", func(t *testing.T) {
		opts := &redis.Options{Addr: "127.0.0.1:6378"} // wrong port
		assert := assert.New(t)
		mt := mocktracer.Start()
		defer mt.Stop()

		client := NewClient(opts, WithService("my-redis"))
		_, err := client.Get("key").Result()

		spans := mt.FinishedSpans()
		assert.Len(spans, 1)
		span := spans[0]

		assert.Equal("redis.command", span.OperationName())
		assert.NotNil(err)
		assert.Equal(err.Error(), span.Tag(ext.ErrorMsg))
		assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
		assert.Equal("6378", span.Tag(ext.TargetPort))
		assert.Equal("get key: ", span.Tag("redis.raw_command"))
		assert.Equal("go-redis/redis", span.Tag(ext.Component))
		assert.Equal(componentName, span.Integration())
		assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
		assert.Equal("redis", span.Tag(ext.DBSystem))
		assert.Equal("0", span.Tag("out.db"))
		assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))
	})

	t.Run("nil", func(t *testing.T) {
		opts := &redis.Options{Addr: "127.0.0.1:6379"}
		assert := assert.New(t)
		mt := mocktracer.Start()
		defer mt.Stop()

		client := NewClient(opts, WithService("my-redis"))
		_, err := client.Get("non_existent_key").Result()

		spans := mt.FinishedSpans()
		assert.Len(spans, 1)
		span := spans[0]

		assert.Equal(redis.Nil, err)
		assert.Equal("redis.command", span.OperationName())
		assert.Zero(span.Tag(ext.ErrorMsg))
		assert.Equal("127.0.0.1", span.Tag(ext.TargetHost))
		assert.Equal("6379", span.Tag(ext.TargetPort))
		assert.Equal("get non_existent_key: ", span.Tag("redis.raw_command"))
		assert.Equal("go-redis/redis", span.Tag(ext.Component))
		assert.Equal(componentName, span.Integration())
		assert.Equal(ext.SpanKindClient, span.Tag(ext.SpanKind))
		assert.Equal("redis", span.Tag(ext.DBSystem))
		assert.Equal("0", span.Tag("out.db"))
		assert.Equal(float64(0), span.Tag(ext.RedisDatabaseIndex))
	})
}
func TestAnalyticsSettings(t *testing.T) {
	assertRate := func(t *testing.T, mt mocktracer.Tracer, rate interface{}, opts ...ClientOption) {
		client := NewClient(&redis.Options{Addr: "127.0.0.1:6379"}, opts...)
		client.Set("test_key", "test_value", 0)
		pipeline := client.Pipeline()
		pipeline.Expire("pipeline_counter", time.Hour)
		pipeline.(*Pipeliner).ExecWithContext(context.Background())

		spans := mt.FinishedSpans()
		assert.Len(t, spans, 2)
		for _, s := range spans {
			assert.Equal(t, rate, s.Tag(ext.EventSampleRate))
		}
	}

	t.Run("defaults", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, nil)
	})

	t.Run("global", func(t *testing.T) {
		t.Skip("global flag disabled")
		mt := mocktracer.Start()
		defer mt.Stop()

		testutils.SetGlobalAnalyticsRate(t, 0.4)

		assertRate(t, mt, 0.4)
	})

	t.Run("enabled", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, 1.0, WithAnalytics(true))
	})

	t.Run("disabled", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, nil, WithAnalytics(false))
	})

	t.Run("override", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		testutils.SetGlobalAnalyticsRate(t, 0.4)

		assertRate(t, mt, 0.23, WithAnalyticsRate(0.23))
	})

	t.Run("zero", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, 0.0, WithAnalyticsRate(0.0))
	})
}

func TestWithContext(t *testing.T) {
	opts := &redis.Options{Addr: "127.0.0.1:6379"}
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	client1 := NewClient(opts, WithService("my-redis"))
	s1, ctx1 := tracer.StartSpanFromContext(context.Background(), "span1.name")
	client1 = client1.WithContext(ctx1)
	s2, ctx2 := tracer.StartSpanFromContext(context.Background(), "span2.name")
	client2 := client1.WithContext(ctx2)
	client1.Set("test_key", "test_value", 0)
	client2.Get("test_key")
	s1.Finish()
	s2.Finish()

	spans := mt.FinishedSpans()
	assert.Len(spans, 4)
	var span1, span2, setSpan, getSpan *mocktracer.Span
	for _, s := range spans {
		switch s.Tag(ext.ResourceName) {
		case "span1.name":
			span1 = s
		case "span2.name":
			span2 = s
		case "set":
			setSpan = s
		case "get":
			getSpan = s
		}
	}
	assert.Equal(ctx1, client1.Context())
	assert.Equal(ctx2, client2.Context())
	assert.NotNil(span1)
	assert.NotNil(span2)
	assert.NotNil(setSpan)
	assert.NotNil(getSpan)
	assert.Equal(span1.SpanID(), setSpan.ParentID())
	assert.Equal(span2.SpanID(), getSpan.ParentID())
}
