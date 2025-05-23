// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestPrioritySampler(t *testing.T) {
	// create a new span with given service/env
	mkSpan := func(svc, env string) *Span {
		s := &Span{service: svc, meta: map[string]string{}}
		if env != "" {
			s.meta["env"] = env
		}
		return s
	}

	t.Run("mkspan", func(t *testing.T) {
		assert := assert.New(t)
		s := mkSpan("my-service", "my-env")
		assert.Equal("my-service", s.service)
		assert.Equal("my-env", s.meta[ext.Environment])

		s = mkSpan("my-service2", "")
		assert.Equal("my-service2", s.service)
		_, ok := s.meta[ext.Environment]
		assert.False(ok)
	})

	t.Run("ops", func(t *testing.T) {
		ps := newPrioritySampler()
		assert := assert.New(t)

		type key struct{ service, env string }
		for _, tt := range []struct {
			in  string
			out map[key]float64
		}{
			{
				in: `{}`,
				out: map[key]float64{
					{"some-service", ""}:       1,
					{"obfuscate.http", "none"}: 1,
				},
			},
			{
				in: `{
					"rate_by_service":{
						"service:,env:":0.8,
						"service:obfuscate.http,env:":0.9,
						"service:obfuscate.http,env:none":0.9
					}
				}`,
				out: map[key]float64{
					{"obfuscate.http", ""}:      0.9,
					{"obfuscate.http", "none"}:  0.9,
					{"obfuscate.http", "other"}: 0.8,
					{"some-service", ""}:        0.8,
				},
			},
			{
				in: `{
					"rate_by_service":{
						"service:my-service,env:":0.2,
						"service:my-service,env:none":0.2
					}
				}`,
				out: map[key]float64{
					{"my-service", ""}:          0.2,
					{"my-service", "none"}:      0.2,
					{"obfuscate.http", ""}:      0.8,
					{"obfuscate.http", "none"}:  0.8,
					{"obfuscate.http", "other"}: 0.8,
					{"some-service", ""}:        0.8,
				},
			},
		} {
			assert.NoError(ps.readRatesJSON(io.NopCloser(strings.NewReader(tt.in))))
			for k, v := range tt.out {
				assert.Equal(v, ps.getRate(mkSpan(k.service, k.env)), k)
			}
		}
	})

	t.Run("race", func(t *testing.T) {
		ps := newPrioritySampler()
		assert := assert.New(t)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				assert.NoError(ps.readRatesJSON(
					io.NopCloser(strings.NewReader(
						`{
							"rate_by_service":{
								"service:,env:":0.8,
								"service:obfuscate.http,env:none":0.9
							}
						}`,
					)),
				))
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				ps.getRate(mkSpan("obfuscate.http", "none"))
				ps.getRate(mkSpan("other.service", "none"))
			}
		}()

		wg.Wait()
	})

	t.Run("apply", func(t *testing.T) {
		ps := newPrioritySampler()
		assert := assert.New(t)
		assert.NoError(ps.readRatesJSON(
			io.NopCloser(strings.NewReader(
				`{
					"rate_by_service":{
						"service:obfuscate.http,env:":0.5,
						"service:obfuscate.http,env:none":0.5
					}
				}`,
			)),
		))

		testSpan1 := newBasicSpan("http.request")
		testSpan1.service = "obfuscate.http"
		testSpan1.traceID = math.MaxUint64 - (math.MaxUint64 / 4)

		ps.apply(testSpan1)
		assert.EqualValues(ext.PriorityAutoKeep, testSpan1.metrics[keySamplingPriority])
		assert.EqualValues(0.5, testSpan1.metrics[keySamplingPriorityRate])

		testSpan1.traceID = math.MaxUint64 - (math.MaxUint64 / 3)
		ps.apply(testSpan1)
		assert.EqualValues(ext.PriorityAutoReject, testSpan1.metrics[keySamplingPriority])
		assert.EqualValues(0.5, testSpan1.metrics[keySamplingPriorityRate])

		testSpan1.service = "other-service"
		testSpan1.traceID = 1
		assert.EqualValues(ext.PriorityAutoReject, testSpan1.metrics[keySamplingPriority])
		assert.EqualValues(0.5, testSpan1.metrics[keySamplingPriorityRate])
	})
}

func TestRateSampler(t *testing.T) {
	assert := assert.New(t)
	assert.True(NewRateSampler(1).Sample(newBasicSpan("test")))
	assert.False(NewRateSampler(0).Sample(newBasicSpan("test")))
	assert.False(NewRateSampler(0).Sample(newBasicSpan("test")))
	assert.False(NewRateSampler(0.99).Sample(nil))
	assert.False(NewRateSampler(0.5).Sample(newSpan("test", "test", "test", 0, 12078589664685934330, 0)))
	assert.True(NewRateSampler(0.5).Sample(newSpan("test", "test", "test", 0, 13794769880582338323, 0)))
	// traceID 5826373039044427785 * knuthFactor = 9223372036854775807 (leveraging the overflow logic)
	// which is 0.5 * MaxUint64
	assert.True(NewRateSampler(0.5).Sample(newSpan("test", "test", "test", 0, 5826373039044427785, 0)))
}

func TestSamplerRates(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1.0, NewRateSampler(1).Rate())
	assert.Equal(0.0, NewRateSampler(0).Rate())
	assert.Equal(0.5, NewRateSampler(0.5).Rate())
	assert.Equal(0.0, NewRateSampler(-1).Rate()) // out of bounds
	assert.Equal(1.0, NewRateSampler(2).Rate())  // out of bounds
}

func TestRateSamplerSetting(t *testing.T) {
	assert := assert.New(t)
	rs := NewRateSampler(1)
	assert.Equal(1.0, rs.Rate())
	rs.SetRate(0.5)
	assert.Equal(0.5, rs.Rate())
}

func TestRuleEnvVars(t *testing.T) {
	t.Run("dd-sample-rate", func(t *testing.T) {
		assert := assert.New(t)
		for _, tt := range []struct {
			in  string
			out float64
		}{
			{in: "", out: math.NaN()},
			{in: "0.0", out: 0.0},
			{in: "0.5", out: 0.5},
			{in: "1.0", out: 1.0},
			{in: "42.0", out: math.NaN()},    // default if out of range
			{in: "1point0", out: math.NaN()}, // default if invalid value
		} {
			t.Setenv("DD_TRACE_SAMPLE_RATE", tt.in)
			c, err := newConfig()
			assert.NoError(err)
			res := c.globalSampleRate
			if math.IsNaN(tt.out) {
				assert.True(math.IsNaN(res))
			} else {
				assert.Equal(tt.out, res)
			}
		}
	})

	t.Run("otel-sample-rate", func(t *testing.T) {
		for _, tt := range []struct {
			config string
			rate   float64
		}{
			{config: "parentbased_always_on", rate: 1.0},
			{config: "parentbased_always_off", rate: 0.0},
			{config: "parentbased_traceidratio", rate: 0.5},
			{config: "always_on", rate: 1.0},
			{config: "always_off", rate: 0.0},
			{config: "traceidratio", rate: 0.75},
		} {
			t.Run(tt.config, func(t *testing.T) {
				assert := assert.New(t)
				t.Setenv("OTEL_TRACES_SAMPLER", tt.config)
				t.Setenv("OTEL_TRACES_SAMPLER_ARG", fmt.Sprintf("%f", tt.rate))
				c, err := newConfig()
				assert.NoError(err)
				res := c.globalSampleRate
				assert.Equal(tt.rate, res)
			})
		}
	})

	t.Run("rate-limit", func(t *testing.T) {
		assert := assert.New(t)
		for _, tt := range []struct {
			in  string
			out *rate.Limiter
		}{
			{in: "", out: rate.NewLimiter(100.0, 100)},
			{in: "0.0", out: rate.NewLimiter(0.0, 0)},
			{in: "0.5", out: rate.NewLimiter(0.5, 1)},
			{in: "1.0", out: rate.NewLimiter(1.0, 1)},
			{in: "42.0", out: rate.NewLimiter(42.0, 42)},
			{in: "-1.0", out: rate.NewLimiter(100.0, 100)},    // default if out of range
			{in: "1point0", out: rate.NewLimiter(100.0, 100)}, // default if invalid value
		} {
			t.Setenv("DD_TRACE_RATE_LIMIT", tt.in)
			c, err := newConfig()
			assert.NoError(err)
			res := newRateLimiter(c.traceRateLimitPerSecond)
			assert.Equal(tt.out, res.limiter)
		}
	})

	t.Run("trace-sampling-rules", func(t *testing.T) {
		assert := assert.New(t)

		tests := []struct {
			value  string
			ruleN  int
			errStr string
		}{
			{
				value: "[]",
				ruleN: 0,
			},
			{
				value: `[{"service": "abcd", "sample_rate": 1.0}]`,
				ruleN: 1,
			},
			{
				value: `[{"service": "abcd", "sample_rate": 1.0},{"name": "wxyz", "sample_rate": 0.9},{"service": "efgh", "name": "lmnop", "sample_rate": 0.42}]`,
				ruleN: 3,
			},
			{
				value: `[{"sample_rate": 1.0,"tags": {"host":"h-1234"}}]`,
				ruleN: 1,
			},
			{
				value: `[{"resource": "root", "sample_rate": 1.0, "tags": {"host":"h-1234"}}]`,
				ruleN: 1,
			},
			{
				value: `[{"sample_rate": 1.0, "tags": {"host":"h-1234"}}]`,
				ruleN: 1,
			},
			{
				// invalid rule ignored
				value:  `[{"service": "abcd", "sample_rate": 42.0}, {"service": "abcd", "sample_rate": 0.2}]`,
				ruleN:  1,
				errStr: "\n\tat index 0: ignoring rule {Service:abcd Rate:42.0}: rate is out of [0.0, 1.0] range",
			},
			{
				// invalid rule ignored
				value:  `[{"service": "abcd", "sample_rate": 42.0}, {"service": "abcd", "sample_rate": 0.2}]`,
				ruleN:  1,
				errStr: "\n\tat index 0: ignoring rule {Service:abcd Rate:42.0}: rate is out of [0.0, 1.0] range",
			},
			{
				value:  `not JSON at all`,
				errStr: "\n\terror unmarshalling JSON: invalid character 'o' in literal null (expecting 'u')",
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				t.Setenv("DD_TRACE_SAMPLING_RULES", test.value)
				rules, _, err := samplingRulesFromEnv()
				if test.errStr == "" {
					assert.NoError(err)
				} else {
					assert.Equal(test.errStr, err.Error())
				}
				assert.Len(rules, test.ruleN, "failed at %d", i)
			})
		}
	})

	t.Run("span-sampling-rules", func(t *testing.T) {
		assert := assert.New(t)

		for i, tt := range []struct {
			value  string
			ruleN  int
			errStr string
		}{
			{
				value: "[]",
				ruleN: 0,
			},
			{
				value: `[{"service": "abcd", "sample_rate": 1.0}]`,
				ruleN: 1,
			},
			{
				value: `[{"sample_rate": 1.0}, {"service": "abcd"}, {"name": "abcd"}, {}]`,
				ruleN: 4,
			},
			{
				value: `[{"service": "abcd", "name": "wxyz"}]`,
				ruleN: 1,
			},
			{
				value: `[{"sample_rate": 1.0}]`,
				ruleN: 1,
			},
			{
				value: `[{"service": "abcd", "sample_rate": 1.0},{"name": "wxyz", "sample_rate": 0.9},{"service": "efgh", "name": "lmnop", "sample_rate": 0.42}]`,
				ruleN: 3,
			},
			{
				// invalid rule ignored
				value:  `[{"service": "abcd", "sample_rate": 42.0}, {"service": "abcd", "sample_rate": 0.2}]`,
				ruleN:  1,
				errStr: "\n\tat index 0: ignoring rule {Service:abcd Rate:42.0}: rate is out of [0.0, 1.0] range",
			},
			{
				value:  `not JSON at all`,
				errStr: "\n\terror unmarshalling JSON: invalid character 'o' in literal null (expecting 'u')",
			},
		} {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				t.Setenv("DD_SPAN_SAMPLING_RULES", tt.value)
				_, rules, err := samplingRulesFromEnv()
				if tt.errStr == "" {
					assert.NoError(err)
				} else {
					assert.Equal(tt.errStr, err.Error())
				}
				assert.Len(rules, tt.ruleN)
			})
		}
	})

	t.Run("span-sampling-rules-regex", func(t *testing.T) {
		assert := assert.New(t)

		for i, tt := range []struct {
			rules         string
			srvRegex      string
			nameRegex     string
			resourceRegex string
			tagsRegex     map[string]string
			rate          float64
		}{
			{
				rules:     `[{"name": "abcd?", "sample_rate": 1.0}]`,
				srvRegex:  "",
				nameRegex: "(?i)^abcd.$",
				rate:      1.0,
			},
			{
				rules:     `[{"sample_rate": 0.5}]`,
				srvRegex:  "",
				nameRegex: "",
				rate:      0.5,
			},
			{
				rules:     `[{"max_per_second":100}]`,
				srvRegex:  "",
				nameRegex: "",
				rate:      1,
			},
			{
				rules:     `[{"name": "abcd?"}]`,
				srvRegex:  "",
				nameRegex: "(?i)^abcd.$",
				rate:      1.0,
			},
			{
				rules:     `[{"service": "*abcd", "sample_rate":0.5}]`,
				nameRegex: "",
				srvRegex:  "(?i)^.*abcd$",
				rate:      0.5,
			},
			{
				rules:     `[{"service": "*abcd", "sample_rate": 0.5}]`,
				nameRegex: "",
				srvRegex:  "(?i)^.*abcd$",
				rate:      0.5,
			},
			{
				rules:         `[{"service": "*abcd", "sample_rate": 0.5,"resource": "root", "tags": {"host":"h-1234*"}}]`,
				resourceRegex: "(?i)^root$",
				tagsRegex:     map[string]string{"host": "(?i)^h-1234.*$"},
				nameRegex:     "",
				srvRegex:      "(?i)^.*abcd$",
				rate:          0.5,
			},
			{
				rules:         `[{"service": "*abcd", "sample_rate": 0.5,"resource": "rsc-[0-9]+" }]`,
				resourceRegex: "(?i)^rsc-\\[0-9\\]\\+$",
				nameRegex:     "",
				srvRegex:      "(?i)^.*abcd$",
				rate:          0.5,
			},
		} {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				t.Setenv("DD_SPAN_SAMPLING_RULES", tt.rules)
				_, rules, err := samplingRulesFromEnv()
				assert.NoError(err)
				if tt.srvRegex == "" {
					assert.Nil(rules[0].Service)
				} else {
					assert.Equal(tt.srvRegex, rules[0].Service.String())
				}
				if tt.nameRegex == "" {
					assert.Nil(rules[0].Name)
				} else {
					assert.Equal(tt.nameRegex, rules[0].Name.String())
				}
				if tt.resourceRegex != "" {
					assert.Equal(tt.resourceRegex, rules[0].Resource.String())
				}
				if tt.tagsRegex != nil {
					for k, v := range tt.tagsRegex {
						assert.Equal(v, rules[0].Tags[k].String())
					}
				}
				assert.Equal(tt.rate, rules[0].Rate)
			})
		}
	})
}

func TestRulesSampler(t *testing.T) {
	makeSpan := func(op string, svc string) *Span {
		s := newSpan(op, svc, "res-10", randUint64(), randUint64(), 0)
		s.setMeta("hostname", "hn-30")
		return s
	}
	makeFinishedSpan := func(op, svc, resource string, tags map[string]interface{}) *Span {
		s := newSpan(op, svc, resource, randUint64(), randUint64(), 0)
		for k, v := range tags {
			s.SetTag(k, v)
		}
		s.finished = true
		return s
	}
	t.Run("no-rules", func(t *testing.T) {
		assert := assert.New(t)
		c, err := newConfig()
		assert.NoError(err)
		rs := newRulesSampler(nil, nil, c.globalSampleRate, c.traceRateLimitPerSecond)

		span := makeSpan("http.request", "test-service")
		result := rs.SampleTrace(span)
		assert.False(result)
	})

	t.Run("matching-trace-rules-env", func(t *testing.T) {
		for _, tt := range []struct {
			rules    string
			spanSrv  string
			spanName string
			spanRsc  string
			spanTags map[string]interface{}
		}{
			{
				rules:   `[{"service": "web.non-matching*", "sample_rate": 0}, {"service": "web*", "sample_rate": 1}]`,
				spanSrv: "web.service",
			},
			{
				rules:    `[{"service": "web.srv", "name":"web.req","sample_rate": 1, "resource": "res/bar"}]`,
				spanSrv:  "web.srv",
				spanName: "web.req",
				spanRsc:  "res/bar",
			},
			{
				rules:   `[{"service": "web.service", "sample_rate": 1}]`,
				spanSrv: "web.service",
			},
			{
				rules:   `[{"resource": "http_*", "sample_rate": 1}]`,
				spanSrv: "web.service",
				spanRsc: "http_rec",
			},
			{
				rules:   `[{"service":"web*", "sample_rate": 1}]`,
				spanSrv: "web.service",
			},
			{
				rules:   `[{"service":"web*", "sample_rate": 1}]`,
				spanSrv: "web.service",
			},
			{
				rules:    `[{"resource": "http_*", "tags":{"host":"COMP-*"}, "sample_rate": 1}]`,
				spanSrv:  "web.service",
				spanRsc:  "http_rec",
				spanTags: map[string]interface{}{"host": "COMP-1234"},
			},
			{
				rules:    `[{"tags":{"host":"COMP-*"}, "sample_rate": 1}]`,
				spanSrv:  "web.service",
				spanTags: map[string]interface{}{"host": "COMP-1234"},
			},
			{
				rules:    `[{"tags":{"host":"COMP-*"}, "sample_rate": 1}]`,
				spanSrv:  "web.service",
				spanTags: map[string]interface{}{"host": "COMP-1234"},
			},
		} {
			t.Run("", func(t *testing.T) {
				t.Setenv("DD_TRACE_SAMPLING_RULES", tt.rules)
				rules, _, err := samplingRulesFromEnv()
				assert.Nil(t, err)

				assert := assert.New(t)
				c, err := newConfig()
				assert.NoError(err)
				rs := newRulesSampler(rules, nil, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeFinishedSpan(tt.spanName, tt.spanSrv, tt.spanRsc, tt.spanTags)

				result := rs.SampleTrace(span)
				assert.True(result)
			})
		}
	})

	t.Run("matching", func(t *testing.T) {
		traceRules := [][]SamplingRule{
			SpanSamplingRules(Rule{ServiceGlob: "test-service", Rate: 1.}),
			SpanSamplingRules(Rule{NameGlob: "http.request", Rate: 1.}),
			SpanSamplingRules(Rule{ServiceGlob: "test-service", NameGlob: "http.request", Rate: 1.}),
			SpanSamplingRules(Rule{ServiceGlob: "test-*", NameGlob: "http.*", Rate: 1.}),
			SpanSamplingRules(Rule{ServiceGlob: "other-service-1", Rate: 0.0}, Rule{ServiceGlob: "other-service-2", Rate: 0.0}, Rule{ServiceGlob: "test-service", Rate: 1.0}),
			SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn-??"}, Rate: 1.0}),
			SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn-3*"}, ResourceGlob: "res-1*", Rate: 1.0}),
			SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn-?0"}, Rate: 1.0}),
		}
		for _, v := range traceRules {
			t.Run("", func(t *testing.T) {
				assert := assert.New(t)
				c, err := newConfig()
				assert.NoError(err)
				rs := newRulesSampler(v, nil, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeSpan("http.request", "test-service")
				result := rs.SampleTrace(span)
				assert.True(result)
				assert.Equal(1.0, span.metrics[keyRulesSamplerAppliedRate])
				assert.Equal(1.0, span.metrics[keyRulesSamplerLimiterRate])
			})
		}
	})

	t.Run("not-matching", func(t *testing.T) {
		traceRules := [][]SamplingRule{
			SpanSamplingRules(Rule{ServiceGlob: "toast-service", Rate: 1.}),
			SpanSamplingRules(Rule{NameGlob: "grpc.request", Rate: 1.}),
			SpanSamplingRules(Rule{NameGlob: "grpc.request", ServiceGlob: "toast-service", Rate: 1.}),
			SpanSamplingRules(Rule{NameGlob: "http\\..*", ServiceGlob: "toast-", Rate: 1.}),
			SpanSamplingRules(Rule{NameGlob: "grpc\\..*", ServiceGlob: "test-", Rate: 1.}),
			SpanSamplingRules(Rule{ServiceGlob: "other-service-1", Rate: 0}, Rule{ServiceGlob: "other-service-2", Rate: 0}, Rule{ServiceGlob: "toast-service", Rate: 1.}),
			SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn--1"}, Rate: 1.0}),
			SpanSamplingRules(Rule{Tags: map[string]string{"host": "hn-1"}, Rate: 1.0}),
			SpanSamplingRules(Rule{ResourceGlob: "res", Rate: 1.0}),
		}
		for _, v := range traceRules {
			t.Run("", func(t *testing.T) {
				assert := assert.New(t)
				c, err := newConfig()
				assert.NoError(err)
				rs := newRulesSampler(v, nil, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeSpan("http.request", "test-service")
				result := rs.SampleTrace(span)
				assert.False(result)
			})
		}
	})

	t.Run("matching-span-rules-from-env", func(t *testing.T) {
		for _, tt := range []struct {
			rules    string
			spanSrv  string
			spanName string
		}{
			{
				rules:    `[{"name": "abcd?", "sample_rate": 1.0, "max_per_second":100}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    `[{"service": "*abcd","max_per_second":100, "sample_rate": 1.0}]`,
				spanSrv:  "xyzabcd",
				spanName: "abcde",
			},
			{
				rules:    `[{"service": "?*", "sample_rate": 1.0, "max_per_second":100}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    `[{"tags":{"hostname":"hn-3*"},"max_per_second":100}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    `[{"resource":"res-1*","max_per_second":100}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
			},
		} {
			t.Run("", func(t *testing.T) {
				t.Setenv("DD_SPAN_SAMPLING_RULES", tt.rules)
				_, rules, err := samplingRulesFromEnv()
				assert.Nil(t, err)
				assert := assert.New(t)
				c, err := newConfig()
				assert.NoError(err)
				rs := newRulesSampler(nil, rules, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeFinishedSpan(tt.spanName, tt.spanSrv, "res-10", map[string]interface{}{"hostname": "hn-30"})

				result := rs.SampleSpan(span)
				assert.True(result)
				assert.Contains(span.metrics, keySpanSamplingMechanism)
				assert.Contains(span.metrics, keySingleSpanSamplingRuleRate)
				assert.Contains(span.metrics, keySingleSpanSamplingMPS)
			})
		}
	})

	t.Run("matching-span-rules", func(t *testing.T) {
		for i, tt := range []struct {
			rules    []SamplingRule
			spanSrv  string
			spanName string
			hasMPS   bool
		}{
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "abcd?", Rate: 1, MaxPerSecond: 100}),
				spanSrv:  "test-service",
				spanName: "abcde",
				hasMPS:   true,
			},
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "abcd?", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "*abcd", Rate: 1, MaxPerSecond: 100}),
				spanSrv:  "xyzabcd",
				spanName: "abcde",
				hasMPS:   true,
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "*abcd", Rate: 1.0}),
				spanSrv:  "xyzabcd",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "abcd?", ServiceGlob: "*service", Rate: 1.0, MaxPerSecond: 100}),
				spanSrv:  "test-service",
				spanName: "abcde",
				hasMPS:   true,
			},
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "abcd?", ServiceGlob: "*service", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "?*", Rate: 1, MaxPerSecond: 100}),
				spanSrv:  "test-service",
				spanName: "abcde",
				hasMPS:   true,
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "?*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, ResourceGlob: "res*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, NameGlob: "abc*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*", "tier": "20?"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*", "tier": "2*"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*", "tier": "*"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*", "tag": "*"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "web*", Rate: 1.0}),
				spanSrv:  "wEbServer",
				spanName: "web.reqUEst",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"shall-pass": "true"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
		} {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				assert := assert.New(t)
				c, err := newConfig(WithSamplingRules(tt.rules))
				assert.NoError(err)
				rs := newRulesSampler(nil, c.spanRules, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeFinishedSpan(tt.spanName, tt.spanSrv, "res-10", map[string]interface{}{"hostname": "hn-30",
					"tag":        20.1,
					"tier":       209,
					"shall-pass": true,
				})
				result := rs.SampleSpan(span)
				assert.True(result)
				assert.Contains(span.metrics, keySpanSamplingMechanism)
				assert.Contains(span.metrics, keySingleSpanSamplingRuleRate)
				if tt.hasMPS {
					assert.Contains(span.metrics, keySingleSpanSamplingMPS)
				}
			})
		}
	})

	t.Run("not-matching-span-rules-from-env", func(t *testing.T) {
		for _, tt := range []struct {
			rules    string
			spanSrv  string
			spanName string
			resName  string
		}{
			{
				//first matching rule takes precedence
				rules:    `[{"name": "abcd?", "sample_rate": 0.0},{"name": "abcd?", "sample_rate": 1.0}]`,
				spanSrv:  "test-service",
				spanName: "abcdef",
				resName:  "res-10",
			},
			{
				rules:    `[{"service": "abcd", "sample_rate": 1.0}]`,
				spanSrv:  "xyzabcd",
				spanName: "abcde",
				resName:  "res-10",
			},
			{
				rules:    `[{"resource": "rc-100", "sample_rate": 1.0}]`,
				spanSrv:  "xyzabcd",
				spanName: "abcde",
				resName:  "external_api",
			},
			{
				rules:    `[{"resource": "rc-100", "sample_rate": 1.0}]`,
				spanSrv:  "xyzabcd",
				spanName: "abcde",
				resName:  "external_api",
			},
			{
				rules:    `[{"service": "?", "sample_rate": 1.0}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
				resName:  "res-10",
			},
			{
				rules:    `[{"tags": {"*":"hs-30"}, "sample_rate": 1.0}]`,
				spanSrv:  "test-service",
				spanName: "abcde",
				resName:  "res-10",
			},
		} {
			t.Run("", func(t *testing.T) {
				t.Setenv("DD_SPAN_SAMPLING_RULES", tt.rules)
				_, rules, _ := samplingRulesFromEnv()

				assert := assert.New(t)
				c, err := newConfig()
				assert.NoError(err)
				rs := newRulesSampler(nil, rules, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeFinishedSpan(tt.spanName, tt.spanSrv, tt.resName, map[string]interface{}{"hostname": "hn-30"})
				result := rs.SampleSpan(span)
				assert.False(result)
				assert.NotContains(span.metrics, keySpanSamplingMechanism)
				assert.NotContains(span.metrics, keySingleSpanSamplingRuleRate)
				assert.NotContains(span.metrics, keySingleSpanSamplingMPS)
			})
		}
	})

	t.Run("not-matching-span-rules", func(t *testing.T) {
		for _, tt := range []struct {
			spanSrv  string
			spanName string
			rules    []SamplingRule
		}{
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "[a-z]+\\d+", ServiceGlob: "^test-[a-z]+", Rate: 1.}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "[a-z]+\\d+", ServiceGlob: "^test-\\w+", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{NameGlob: "[a-z]+\\d+", ServiceGlob: "\\w+", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "\\w+", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"host": "hn-1"}, Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn-100"}, ResourceGlob: "res-1*", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn-10"}, ResourceGlob: "res-100", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{ServiceGlob: "\\w+", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde123",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "incorrect*"}, Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, ResourceGlob: "resnope*", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, NameGlob: "abcno", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, ServiceGlob: "test234", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*"}, NameGlob: "abc234", ServiceGlob: "testno", Rate: 1.0}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},

			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"tag": "20"}, Rate: 1}),
				spanSrv:  "wEbServer",
				spanName: "web.reqUEst",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"tag": "2*"}, Rate: 1}),
				spanSrv:  "wEbServer",
				spanName: "web.reqUEst",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"tag": "2?"}, Rate: 1}),
				spanSrv:  "wEbServer",
				spanName: "web.reqUEst",
			},
			{
				rules:    SpanSamplingRules(Rule{Tags: map[string]string{"hostname": "hn*", "tag": "2*"}, NameGlob: "abc*", ServiceGlob: "test*", Rate: 1}),
				spanSrv:  "test-service",
				spanName: "abcde",
			},
		} {
			t.Run("", func(t *testing.T) {
				assert := assert.New(t)
				c, err := newConfig(WithSamplingRules(tt.rules))
				assert.NoError(err)
				rs := newRulesSampler(nil, c.spanRules, c.globalSampleRate, c.traceRateLimitPerSecond)

				span := makeFinishedSpan(tt.spanName, tt.spanSrv, "res-10", map[string]interface{}{"hostname": "hn-30",
					"tag": 20.1,
				})
				result := rs.SampleSpan(span)
				assert.False(result)
				assert.NotContains(span.metrics, keySpanSamplingMechanism)
				assert.NotContains(span.metrics, keySingleSpanSamplingRuleRate)
				assert.NotContains(span.metrics, keySingleSpanSamplingMPS)
			})
		}
	})

	t.Run("default-rate", func(t *testing.T) {
		ruleSets := [][]SamplingRule{
			{},
			TraceSamplingRules(Rule{ServiceGlob: "other-service"}),
		}
		for _, rules := range ruleSets {
			sampleRates := []float64{
				0.0,
				0.8,
				1.0,
			}
			for _, rate := range sampleRates {
				t.Run("", func(t *testing.T) {
					assert := assert.New(t)
					t.Setenv("DD_TRACE_SAMPLE_RATE", fmt.Sprint(rate))
					c, err := newConfig()
					assert.NoError(err)
					rs := newRulesSampler(nil, rules, c.globalSampleRate, c.traceRateLimitPerSecond)

					span := makeSpan("http.request", "test-service")
					result := rs.SampleTrace(span)
					assert.False(result)
					result = rs.SampleTraceGlobalRate(span)
					assert.True(result)
					assert.Equal(rate, span.metrics[keyRulesSamplerAppliedRate])
					if rate > 0.0 && (span.metrics[keySamplingPriority] != ext.PriorityUserReject) {
						assert.Equal(1.0, span.metrics[keyRulesSamplerLimiterRate])
					}
				})
			}
		}
	})

	// this test actually starts the span to verify that tag sampling works regardless of how
	// the tags where set (during the Start func, or via s.SetTag())
	// previously, sampling was ran once during creation, so this test would fail.
	t.Run("rules-with-start-span", func(t *testing.T) {
		testEnvs := []struct {
			rules            string
			generalRate      string
			samplingPriority float64
			appliedRate      float64
		}{
			{
				rules:            `[{"tags": {"tag1": "non-matching"}, "sample_rate": 0}, {"resource": "/bar", "sample_rate": 1}]`,
				generalRate:      "0",
				samplingPriority: 2,
				appliedRate:      1,
			},
			{
				rules:            `[{"tags": {"tag1": "non-matching"}, "sample_rate": 0}, {"tags": {"tag1": "val1"}, "sample_rate": 1}]`,
				generalRate:      "0",
				samplingPriority: 2,
				appliedRate:      1,
			},
			{
				rules:            `[ {"tags": {"tag1": "val1"}, "sample_rate": 0}]`,
				generalRate:      "1",
				samplingPriority: -1,
				appliedRate:      0,
			},
			{
				rules:            `  [{"service": "webserver", "name": "web.request", "sample_rate": 0}]`,
				generalRate:      "1",
				samplingPriority: -1,
				appliedRate:      0,
			},
		}

		for _, test := range testEnvs {
			t.Run("", func(t *testing.T) {
				t.Setenv("DD_TRACE_SAMPLING_RULES", test.rules)
				t.Setenv("DD_TRACE_SAMPLE_RATE", test.generalRate)
				_, _, _, stop, err := startTestTracer(t)
				assert.NoError(t, err)
				defer stop()

				s, _ := StartSpanFromContext(context.Background(), "web.request",
					ServiceName("webserver"), ResourceName("/bar"))
				s.SetTag("tag1", "val1")
				s.SetTag("tag2", "val2")
				s.Finish()

				assert.EqualValues(t, s.metrics[keySamplingPriority], test.samplingPriority)
				assert.EqualValues(t, s.metrics[keyRulesSamplerAppliedRate], test.appliedRate)
			})
		}
	})

	t.Run("locked-sampling-before-propagating-context", func(t *testing.T) {
		t.Setenv("DD_TRACE_SAMPLING_RULES",
			`[{"tags": {"tag2": "val2"}, "sample_rate": 0},{"tags": {"tag1": "val1"}, "sample_rate": 1},{"tags": {"tag0": "val*"}, "sample_rate": 0}]`)
		t.Setenv("DD_TRACE_SAMPLE_RATE", "0")
		tr, _, _, stop, err := startTestTracer(t)
		assert.NoError(t, err)
		defer stop()

		originSpan, _ := StartSpanFromContext(context.Background(), "web.request",
			ServiceName("webserver"), ResourceName("/bar"), Tag("tag0", "val0"))
		originSpan.SetTag("tag1", "val1")
		// based on the  Tag("tag0", "val0") start span option, span sampling would be 'drop',
		// and setting the second pair of tags doesn't invoke sampling func
		assert.EqualValues(t, -1, originSpan.metrics[keySamplingPriority])
		assert.EqualValues(t, 0, originSpan.metrics[keyRulesSamplerAppliedRate])
		headers := TextMapCarrier(map[string]string{})

		// inject invokes resampling, since span satisfies rule #2, sampling will be 'keep'
		err = tr.Inject(originSpan.Context(), headers)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, originSpan.metrics[keySamplingPriority])
		assert.EqualValues(t, 1, originSpan.metrics[keyRulesSamplerAppliedRate])

		// context already injected / propagated, and the sampling decision can no longer be changed
		originSpan.SetTag("tag2", "val2")
		originSpan.Finish()
		assert.EqualValues(t, 2, originSpan.metrics[keySamplingPriority])
		assert.EqualValues(t, 1, originSpan.metrics[keyRulesSamplerAppliedRate])

		w3cCtx, err := tr.Extract(headers)
		assert.Nil(t, err)

		w3cSpan, _ := StartSpanFromContext(context.Background(), "web.request", ChildOf(w3cCtx))
		w3cSpan.Finish()

		assert.EqualValues(t, 2, w3cSpan.metrics[keySamplingPriority])
	})

	t.Run("manual keep priority", func(t *testing.T) {
		t.Setenv("DD_TRACE_SAMPLING_RULES", `[{"resource": "keep_me", "sample_rate": 0}]`)
		_, _, _, stop, err := startTestTracer(t)
		assert.NoError(t, err)
		defer stop()

		s, _ := StartSpanFromContext(context.Background(), "whatever")
		s.SetTag(ext.ManualKeep, true)
		s.SetTag(ext.ResourceName, "keep_me")
		s.Finish()
		assert.EqualValues(t, s.metrics[keySamplingPriority], 2)
	})

	t.Run("no-agent_psr-with-rules-sampling", func(t *testing.T) {
		t.Setenv("DD_TRACE_SAMPLING_RULES", `[{"resource": "keep_me", "sample_rate": 0}]`)
		_, _, _, stop, err := startTestTracer(t)
		assert.NoError(t, err)
		defer stop()

		span, _ := StartSpanFromContext(context.Background(), "whatever")
		span.SetTag(ext.ResourceName, "keep_me")
		span.Finish()
		assert.NotContains(t, span.metrics, keySamplingPriorityRate)
		assert.Contains(t, span.metrics, keyRulesSamplerAppliedRate)
	})
}

func TestSamplingRuleUnmarshal(t *testing.T) {
	isEqual := func(actual, expected SamplingRule) error {
		if actual.Service != nil && actual.Service.String() != expected.Service.String() {
			return fmt.Errorf("service: %s != %s", actual.Service.String(), expected.Service.String())
		}
		if actual.Name != nil && actual.Name.String() != expected.Name.String() {
			return fmt.Errorf("name: %s != %s", actual.Name.String(), expected.Name.String())
		}
		if actual.Resource != nil && actual.Resource.String() != expected.Resource.String() {
			return fmt.Errorf("resource: %s != %s", actual.Resource.String(), expected.Resource.String())
		}
		if actual.Rate != expected.Rate {
			return fmt.Errorf("rate: %v != %v", actual.Rate, expected.Rate)
		}
		if len(actual.Tags) != len(expected.Tags) {
			return fmt.Errorf("tags length is not equal: %v != %v", len(actual.Tags), len(expected.Tags))
		}
		for k, v := range actual.Tags {
			if v.String() != expected.Tags[k].String() {
				return fmt.Errorf("tag %s: %s != %s", k, v.String(), expected.Tags[k].String())
			}
		}
		if actual.ruleType != expected.ruleType {
			return fmt.Errorf("ruleType: %v != %v", actual.ruleType, expected.ruleType)
		}
		return nil
	}
	t.Run("unmarshal", func(t *testing.T) {
		for i, tt := range []struct {
			rule     string
			expected SamplingRule
		}{
			{
				rule: `{"service": "web.service", "sample_rate": 1.0}`,
				expected: SamplingRule{
					Service:  globMatch("web.service"),
					Name:     globMatch(""),
					Resource: globMatch(""),
					Tags:     map[string]*regexp.Regexp{},
					Rate:     1,
				},
			},
			{
				rule: `{"service": "web.service","type":1, "sample_rate": 1.0}`,
				expected: SamplingRule{
					Service:  globMatch("web.service"),
					Name:     globMatch(""),
					Resource: globMatch(""),
					Tags:     map[string]*regexp.Regexp{},
					Rate:     1,
					ruleType: SamplingRuleTrace,
				},
			},
			{
				rule: `{"name": "web.request", "sample_rate": 1.0}`,
				expected: SamplingRule{
					Name:     globMatch("web.request"),
					Service:  globMatch(""),
					Resource: globMatch(""),
					Tags:     map[string]*regexp.Regexp{},
					Rate:     1,
				},
			},
			{
				rule: `{"resource": "web.resource", "sample_rate": 1.0}`,
				expected: SamplingRule{
					Service:  globMatch(""),
					Name:     globMatch(""),
					Resource: globMatch("web.resource"),
					Tags:     map[string]*regexp.Regexp{},
					Rate:     1,
				},
			},
			{
				rule: `{"tags": {"host": "hn-30"}, "sample_rate": 1.0}`,

				expected: SamplingRule{
					Service:  globMatch(""),
					Name:     globMatch(""),
					Resource: globMatch(""),
					Tags:     map[string]*regexp.Regexp{"host": globMatch("hn-30")},
					Rate:     1,
				},
			},
			{
				rule: `{"service": "web.service", "name": "web.request", "sample_rate": 1.0}`,
				expected: SamplingRule{
					Service:  globMatch("web.service"),
					Name:     globMatch("web.request"),
					Resource: globMatch(""),
					Tags:     nil,
					Rate:     1,
				},
			},
		} {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				var r SamplingRule
				err := r.UnmarshalJSON([]byte(tt.rule))
				assert.Nil(t, err)
				assert.NoError(t, isEqual(r, tt.expected))
			})
		}
	})

}

func TestRulesSamplerConcurrency(t *testing.T) {
	rules := TraceSamplingRules(
		Rule{ServiceGlob: "test-service", Rate: 1.0},
		Rule{NameGlob: "db.query", ServiceGlob: "postgres.db", Rate: 1.0},
		Rule{NameGlob: "notweb.request", Rate: 1.0},
	)
	tracer, err := newTracer(WithSamplingRules(rules))
	assert.NoError(t, err)
	defer tracer.Stop()
	span := func(wg *sync.WaitGroup) {
		defer wg.Done()
		tracer.StartSpan("db.query", ServiceName("postgres.db")).Finish()
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go span(wg)
	}
	wg.Wait()
}

func TestRulesSamplerInternals(t *testing.T) {
	makeSpanAt := func(op string, svc string, ts time.Time) *Span {
		s := newSpan(op, svc, "", 0, 0, 0)
		s.start = ts.UnixNano()
		return s
	}

	t.Run("zero-rate", func(t *testing.T) {
		assert := assert.New(t)
		now := time.Now()
		rs := &rulesSampler{}
		span := makeSpanAt("http.request", "test-service", now)
		rs.traces.applyRate(span, 0.0, now, samplernames.RuleRate)
		assert.Equal(0.0, span.metrics[keyRulesSamplerAppliedRate])
		_, ok := span.metrics[keyRulesSamplerLimiterRate]
		assert.False(ok)
	})

	t.Run("full-rate", func(t *testing.T) {
		assert := assert.New(t)
		now := time.Now()
		c, err := newConfig()
		assert.NoError(err)
		rs := newRulesSampler(nil, nil, c.globalSampleRate, c.traceRateLimitPerSecond)
		// set samplingLimiter to specific state
		rs.traces.limiter.prevTime = now.Add(-1 * time.Second)
		rs.traces.limiter.allowed = 1
		rs.traces.limiter.seen = 1

		span := makeSpanAt("http.request", "test-service", now)
		rs.traces.applyRate(span, 1.0, now, samplernames.RuleRate)
		assert.Equal(1.0, span.metrics[keyRulesSamplerAppliedRate])
		assert.Equal(1.0, span.metrics[keyRulesSamplerLimiterRate])
	})

	t.Run("limited-rate", func(t *testing.T) {
		assert := assert.New(t)
		now := time.Now()
		c, err := newConfig()
		assert.NoError(err)
		rs := newRulesSampler(nil, nil, c.globalSampleRate, c.traceRateLimitPerSecond)
		// force sampling limiter to 1.0 spans/sec
		rs.traces.limiter.limiter = rate.NewLimiter(rate.Limit(1.0), 1)
		rs.traces.limiter.prevTime = now.Add(-1 * time.Second)
		rs.traces.limiter.allowed = 2
		rs.traces.limiter.seen = 2
		// first span kept, second dropped
		span := makeSpanAt("http.request", "test-service", now)
		rs.traces.applyRate(span, 1.0, now, samplernames.RuleRate)
		assert.EqualValues(ext.PriorityUserKeep, span.metrics[keySamplingPriority])
		assert.Equal(1.0, span.metrics[keyRulesSamplerAppliedRate])
		assert.Equal(1.0, span.metrics[keyRulesSamplerLimiterRate])
		span = makeSpanAt("http.request", "test-service", now)
		rs.traces.applyRate(span, 1.0, now, samplernames.RuleRate)
		assert.EqualValues(ext.PriorityUserReject, span.metrics[keySamplingPriority])
		assert.Equal(1.0, span.metrics[keyRulesSamplerAppliedRate])
		assert.Equal(0.75, span.metrics[keyRulesSamplerLimiterRate])
	})
}

func TestSamplingLimiter(t *testing.T) {
	t.Run("resets-every-second", func(t *testing.T) {
		assert := assert.New(t)
		sl := newRateLimiter(defaultRateLimit)
		sl.prevSeen = 100
		sl.prevAllowed = 99
		sl.allowed = 42
		sl.seen = 100
		// exact point it should reset
		now := time.Now().Add(1 * time.Second)

		sampled, _ := sl.allowOne(now)
		assert.True(sampled)
		assert.Equal(42.0, sl.prevAllowed)
		assert.Equal(100.0, sl.prevSeen)
		assert.Equal(now, sl.prevTime)
		assert.Equal(1.0, sl.seen)
		assert.Equal(1.0, sl.allowed)
	})

	t.Run("averages-rates", func(t *testing.T) {
		assert := assert.New(t)
		sl := newRateLimiter(defaultRateLimit)
		sl.prevSeen = 100
		sl.prevAllowed = 42
		sl.allowed = 41
		sl.seen = 99
		// this event occurs within the current period
		now := sl.prevTime

		sampled, rate := sl.allowOne(now)
		assert.True(sampled)
		assert.Equal(0.42, rate)
		assert.Equal(now, sl.prevTime)
		assert.Equal(100.0, sl.seen)
		assert.Equal(42.0, sl.allowed)
	})

	t.Run("discards-rate", func(t *testing.T) {
		assert := assert.New(t)
		sl := newRateLimiter(defaultRateLimit)
		sl.prevSeen = 100
		sl.prevAllowed = 42
		sl.allowed = 42
		sl.seen = 100
		// exact point it should discard previous rate
		now := time.Now().Add(2 * time.Second)

		sampled, _ := sl.allowOne(now)
		assert.True(sampled)
		assert.Equal(0.0, sl.prevSeen)
		assert.Equal(0.0, sl.prevAllowed)
		assert.Equal(now, sl.prevTime)
		assert.Equal(1.0, sl.seen)
		assert.Equal(1.0, sl.allowed)
	})
}

func BenchmarkRulesSampler(b *testing.B) {
	const batchSize = 500

	benchmarkStartSpan := func(b *testing.B, t *tracer) {
		SetGlobalTracer(t)
		defer func() {
			SetGlobalTracer(&NoopTracer{})
		}()
		t.prioritySampling.readRatesJSON(io.NopCloser(strings.NewReader(
			`{
                                        "rate_by_service":{
                                                "service:obfuscate.http,env:":0.5,
                                                "service:obfuscate.http,env:none":0.5
                                        }
                                }`,
		)),
		)
		spans := make([]*Span, batchSize)
		b.StopTimer()
		b.ResetTimer()
		for i := 0; i < b.N; i += batchSize {
			n := batchSize
			if i+batchSize > b.N {
				n = b.N - i
			}
			b.StartTimer()
			for j := 0; j < n; j++ {
				spans[j] = t.StartSpan("web.request")
			}
			b.StopTimer()
			for j := 0; j < n; j++ {
				spans[j].Finish()
			}
			d := 0
			for len(t.out) > 0 {
				<-t.out
				d++
			}
		}
	}

	b.Run("no-rules", func(b *testing.B) {
		tracer, err := newUnstartedTracer()
		assert.NoError(b, err)
		benchmarkStartSpan(b, tracer)
	})

	b.Run("unmatching-rules", func(b *testing.B) {
		rules := TraceSamplingRules(
			Rule{ServiceGlob: "test-service", Rate: 1.0},
			Rule{NameGlob: "db.query", ServiceGlob: "postgres.db", Rate: 1.0},
			Rule{NameGlob: "notweb.request", Rate: 1.0})
		tracer, err := newUnstartedTracer(WithSamplingRules(rules))
		assert.NoError(b, err)
		benchmarkStartSpan(b, tracer)
	})

	b.Run("matching-rules", func(b *testing.B) {
		rules := TraceSamplingRules(
			Rule{ServiceGlob: "test-service", Rate: 1.0},
			Rule{NameGlob: "db.query", ServiceGlob: "postgres.db", Rate: 1.0},
			Rule{NameGlob: "web.request", Rate: 1.0})
		tracer, err := newUnstartedTracer(WithSamplingRules(rules))
		assert.NoError(b, err)
		benchmarkStartSpan(b, tracer)
	})

	b.Run("mega-rules", func(b *testing.B) {
		rules := TraceSamplingRules([]Rule{
			{ServiceGlob: "test-service", Rate: 1.0},
			{ServiceGlob: "postgres.db", NameGlob: "db.query", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "notweb.request", Rate: 1.0},
			{NameGlob: "web.request", Rate: 1.0},
		}...)

		tracer, err := newUnstartedTracer(WithSamplingRules(rules))
		assert.NoError(b, err)
		benchmarkStartSpan(b, tracer)
	})
}

func TestGlobMatch(t *testing.T) {
	for i, tt := range []struct {
		pattern     string
		input       string
		shouldMatch bool
	}{
		// pattern with *
		{"test*", "test", true},
		{"test*", "test-case", true},
		{"test*", "a-test", false},
		{"*test", "a-test", true},
		{"a*case", "acase", true},
		{"a*case", "a-test-case", true},
		{"a*test*case", "a-test-case", true},
		{"a*test*case", "atestcase", true},
		{"a*test*case", "abadcase", false},
		{"*a*a*a*a*a*a", "aaaaaaaaaaaaaaaaaaaaaaaaaax", false},
		{"*a*a*a*a*a*a", "aaaaaaaarrrrrrraaaraaarararaarararaarararaaa", true},
		// pattern with ?
		{"test?", "test", false},
		{"test?", "test-case", false},
		{"test?", "a-test", false},
		{"?test", "a-test", false},
		{"a?case", "acase", false},
		{"a?case", "a-case", true},
		{"a?test?case", "a-test-case", true},
		{"a?test?case", "a-test--case", false},
		// pattern with ? and *
		{"?test*", "atest", true},
		{"?test*", "atestcase", true},
		{"?test*", "testcase", false},
		{"?test*", "testcase", false},
		{"test*case", "testcase", true},
		{"a?test*", "a-test-case", true},
		{"a?test*", "atestcase", false},
		{"a*test?", "a-test-", true},
		{"a*test?", "atestcase", false},
		{"a*test?case", "a--test-case", true},
		{"a*test?case", "a--test--case", false},
		{"a?test*case", "a-testing--case", true},
		{"the?test*case", "the-test-cases", false},
		// valid non-glob regex pattern
		{`[a-z]+\\d+`, "abc123", false},
		{`[a-z]+\\d+`, `[a-z]+\\d+`, true},
		{`\\w+`, `\\w+`, true},
		{`\\w+`, `abc123`, false},
		{"*/*", `a/123`, true},
		{`*\/*`, `a\/123`, true},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			rg := globMatch(tt.pattern)
			if tt.shouldMatch {
				assert.Regexp(t, rg, tt.input)
			} else {
				assert.NotRegexp(t, rg, tt.input)
			}
		})
	}
}

func TestSamplingRuleMarshall(t *testing.T) {
	for i, tt := range []struct {
		in       Rule
		ruleType SamplingRuleType
		out      string
	}{
		{Rule{ServiceGlob: "srv.*"}, SamplingRuleTrace, `{"service":"srv.*","sample_rate":0}`},
		{Rule{NameGlob: "ops.*", ServiceGlob: "srv.*"}, SamplingRuleTrace, `{"service":"srv.*","name":"ops.*","sample_rate":0}`},
		{Rule{NameGlob: "ops.*", ServiceGlob: "srv.*", Rate: 0.55}, SamplingRuleTrace, `{"service":"srv.*","name":"ops.*","sample_rate":0.55}`},
		{Rule{Tags: nil, ResourceGlob: "http_get", Rate: 0.55}, SamplingRuleTrace, `{"resource":"http_get","sample_rate":0.55}`},
		{Rule{Tags: map[string]string{"host": "hn-*"}, ResourceGlob: "http_get", Rate: 0.35}, SamplingRuleTrace, `{"resource":"http_get","sample_rate":0.35,"tags":{"host":"hn-*"}}`},
		{Rule{NameGlob: "ops.*", ServiceGlob: "srv.*", Rate: 0.55}, SamplingRuleSpan, `{"service":"srv.*","name":"ops.*","sample_rate":0.55}`},
		{Rule{NameGlob: "ops.*", ServiceGlob: "srv.*", Rate: 0.55, MaxPerSecond: 1000}, SamplingRuleSpan, `{"service":"srv.*","name":"ops.*","sample_rate":0.55,"max_per_second":1000}`},
		{Rule{Tags: nil, ResourceGlob: "//bar", Rate: 1}, SamplingRuleTrace, `{"resource":"//bar","sample_rate":1}`},
		{Rule{Tags: map[string]string{"tag_key": "tag_value.*"}, ResourceGlob: "//bar", Rate: 1}, SamplingRuleTrace, `{"resource":"//bar","sample_rate":1,"tags":{"tag_key":"tag_value.*"}}`},
	} {
		var sr SamplingRule
		switch tt.ruleType {
		case SamplingRuleTrace:
			sr = TraceSamplingRules(tt.in)[0]
		case SamplingRuleSpan:
			sr = SpanSamplingRules(tt.in)[0]
		}
		m, err := sr.MarshalJSON()
		assert.Nil(t, err)
		assert.Equal(t, tt.out, string(m), "at %d index", i)
	}
}

func TestSamplingRuleMarshallGlob(t *testing.T) {
	for i, tt := range []struct {
		pattern string
		input   string
		rgx     *regexp.Regexp
		marshal string
	}{
		// pattern with *
		{"test*", "test", regexp.MustCompile("(?i)^test.*$"), `{"service":"test*","sample_rate":1}`},
		{"*test", "a-test", regexp.MustCompile("(?i)^.*test$"), `{"service":"*test","sample_rate":1}`},
		{"a*case", "acase", regexp.MustCompile("(?i)^a.*case$"), `{"service":"a*case","sample_rate":1}`},
		// pattern regexp.MustCompile(), ``, with ?
		{"a?case", "a-case", regexp.MustCompile("(?i)^a.case$"), `{"service":"a?case","sample_rate":1}`},
		{"a?test?case", "a-test-case", regexp.MustCompile("(?i)^a.test.case$"), `{"service":"a?test?case","sample_rate":1}`},
		//// pattern with ? regexp.MustCompile(), ``, and *
		{"?test*", "atest", regexp.MustCompile("(?i)^.test.*$"), `{"service":"?test*","sample_rate":1}`},
		{"test*case", "testcase", regexp.MustCompile("(?i)^test.*case$"), `{"service":"test*case","sample_rate":1}`},
		{"a?test*", "a-test-case", regexp.MustCompile("(?i)^a.test.*$"), `{"service":"a?test*","sample_rate":1}`},
		{"a*test?", "a-test-", regexp.MustCompile("(?i)^a.*test.$"), `{"service":"a*test?","sample_rate":1}`},
		{"a*test?case", "a--test-case", regexp.MustCompile("(?i)^a.*test.case$"), `{"service":"a*test?case","sample_rate":1}`},
		{"a?test*case", "a-testing--case", regexp.MustCompile("(?i)^a.test.*case$"), `{"service":"a?test*case","sample_rate":1}`},
		//// valid non-glob regex regexp.MustCompile(), ``, pattern
		{"*/*", `a/123`, regexp.MustCompile("(?i)^.*/.*$"), `{"service":"*/*","sample_rate":1}`},
		{`*\/*`, `a\/123`, regexp.MustCompile("(?i)^.*/.*$"), `{"service":"*/*","sample_rate":1}`},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			// the goal of this test is
			// 1. to verify that the glob pattern is correctly converted to a regex
			// 2. to verify that the rule is correctly marshalled

			rules, _ := unmarshalSamplingRules([]byte(fmt.Sprintf(`[{"service": "%s", "sample_rate": 1.0}]`, tt.pattern)),
				SamplingRuleTrace)
			rule := rules[0]

			assert.Regexp(t, rules[0].Service, tt.input)
			assert.Equal(t, tt.rgx.String(), rule.Service.String())

			m, err := rule.MarshalJSON()
			assert.Nil(t, err)
			assert.Equal(t, tt.marshal, string(m))
		})
	}
}

func BenchmarkGlobMatchSpan(b *testing.B) {
	var spans []*Span
	for i := 0; i < 1000; i++ {
		spans = append(spans, newSpan("name.ops.date", "srv.name.ops.date", "", 0, 0, 0))
	}

	b.Run("no-regex", func(b *testing.B) {
		b.Setenv("DD_SPAN_SAMPLING_RULES", `[{"service": "srv.name.ops.date", "name": "name.ops.date?", "sample_rate": 0.234}]`)
		_, rules, err := samplingRulesFromEnv()
		assert.Nil(b, err)
		rs := newSingleSpanRulesSampler(rules)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, span := range spans {
				rs.apply(span)
			}
		}
	})

	b.Run("glob-match-?", func(b *testing.B) {
		b.Setenv("DD_SPAN_SAMPLING_RULES", `[{"service": "srv?name?ops?date", "name": "name*ops*date*", "sample_rate": 0.234}]`)
		_, rules, err := samplingRulesFromEnv()
		assert.Nil(b, err)
		rs := newSingleSpanRulesSampler(rules)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, span := range spans {
				rs.apply(span)
			}
		}
	})

	b.Run("glob-match-*", func(b *testing.B) {
		b.Setenv("DD_SPAN_SAMPLING_RULES", `[{"service": "srv*name*ops*date", "name": "name?ops?date?", "sample_rate": 0.234}]`)

		_, rules, err := samplingRulesFromEnv()
		assert.Nil(b, err)
		rs := newSingleSpanRulesSampler(rules)

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, span := range spans {
				rs.apply(span)
			}
		}
	})
}

func TestSetGlobalSampleRate(t *testing.T) {
	rs := newTraceRulesSampler(nil, math.NaN(), defaultRateLimit)
	assert.True(t, math.IsNaN(rs.globalRate))

	// Comparing NaN values
	b := rs.setGlobalSampleRate(math.NaN())
	assert.True(t, math.IsNaN(rs.globalRate))
	assert.False(t, b)

	// valid
	b = rs.setGlobalSampleRate(0.5)
	assert.Equal(t, 0.5, rs.globalRate)
	assert.True(t, b)

	// valid
	b = rs.setGlobalSampleRate(0.0)
	assert.Equal(t, 0.0, rs.globalRate)
	assert.True(t, b)

	// ignore out of bound value
	b = rs.setGlobalSampleRate(2)
	assert.Equal(t, 0.0, rs.globalRate)
	assert.False(t, b)
}

func TestSampleTagsRootOnly(t *testing.T) {

	t.Run("no-ctx-propagation", func(t *testing.T) {
		assert := assert.New(t)
		Start(WithSamplingRules(TraceSamplingRules(
			Rule{Tags: map[string]string{"tag": "20"}, Rate: 1},
			Rule{ResourceGlob: "root"},
		)))
		tr := GetGlobalTracer()
		defer tr.Stop()

		root := tr.StartSpan("mysql.root", ResourceName("root"))
		child := tr.StartSpan("mysql.child", ChildOf(root.Context()))
		child.SetTag("tag", 20)

		// root span should be sampled with the second rule
		// sampling decision is 0, thus "_dd.limit_psr" is not present
		assert.Contains(root.metrics, keyRulesSamplerAppliedRate)
		assert.Equal(0., root.metrics[keyRulesSamplerAppliedRate])
		assert.NotContains(root.metrics, keyRulesSamplerLimiterRate)

		// neither"_dd.limit_psr", nor "_dd.rule_psr" should be present
		// on the child span
		assert.NotContains(child.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(child.metrics, keyRulesSamplerLimiterRate)

		// setting this tag would change the result of sampling,
		// which will occur after the span is finished
		root.SetTag("tag", 20)
		child.Finish()

		// first sampling rule is applied, the sampling decision is 1
		// and the "_dd.limit_psr" is present
		root.Finish()
		assert.Equal(1., root.metrics[keyRulesSamplerAppliedRate])
		assert.Contains(root.metrics, keyRulesSamplerLimiterRate)

		// neither"_dd.limit_psr", nor "_dd.rule_psr" should be present
		// on the child span
		assert.NotContains(child.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(child.metrics, keyRulesSamplerLimiterRate)
	})

	t.Run("with-ctx-propagation", func(t *testing.T) {
		assert := assert.New(t)
		Start(WithSamplingRules(TraceSamplingRules(
			Rule{Tags: map[string]string{"tag": "20"}, Rate: 1},
			Rule{Tags: nil, ResourceGlob: "root"},
		)))
		tr := GetGlobalTracer()
		defer tr.Stop()

		root := tr.StartSpan("mysql.root", ResourceName("root"))
		child := tr.StartSpan("mysql.child", ChildOf(root.Context()))
		child.SetTag("tag", 20)

		// root span should be sampled with the second rule
		// sampling decision is 0, thus "_dd.limit_psr" is not present
		assert.Equal(0., root.metrics[keyRulesSamplerAppliedRate])
		assert.Contains(root.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(root.metrics, keyRulesSamplerLimiterRate)

		// neither"_dd.limit_psr", nor "_dd.rule_psr" should be present
		// on the child span
		assert.NotContains(child.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(child.metrics, keyRulesSamplerLimiterRate)

		// context propagation locks the span, so no re-sampling should occur
		tr.Inject(root.Context(), TextMapCarrier(map[string]string{}))
		root.SetTag("tag", 20)

		child.Finish()

		// re-sampling should not occur
		root.Finish()
		assert.NotContains(child.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(root.metrics, keyRulesSamplerLimiterRate)

		// neither"_dd.limit_psr", nor "_dd.rule_psr" should be present
		// on the child span
		assert.NotContains(child.metrics, keyRulesSamplerAppliedRate)
		assert.NotContains(child.metrics, keyRulesSamplerLimiterRate)
	})
}
