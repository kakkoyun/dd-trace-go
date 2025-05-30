// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package datastreams

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"

	"github.com/DataDog/dd-trace-go/v2/internal"

	"github.com/tinylib/msgp/msgp"
)

type httpTransport struct {
	url     string            // the delivery URL for stats
	client  *http.Client      // the HTTP client used in the POST
	headers map[string]string // the Transport headers
}

func newHTTPTransport(agentURL *url.URL, client *http.Client) *httpTransport {
	// initialize the default EncoderPool with Encoder headers
	defaultHeaders := map[string]string{
		"Datadog-Meta-Lang":             "go",
		"Datadog-Meta-Lang-Version":     strings.TrimPrefix(runtime.Version(), "go"),
		"Datadog-Meta-Lang-Interpreter": runtime.Compiler + "-" + runtime.GOARCH + "-" + runtime.GOOS,
		"Content-Type":                  "application/msgpack",
		"Content-Encoding":              "gzip",
	}
	if cid := internal.ContainerID(); cid != "" {
		defaultHeaders["Datadog-Container-ID"] = cid
	}
	if entityID := internal.ContainerID(); entityID != "" {
		defaultHeaders["Datadog-Entity-ID"] = entityID
	}
	url := fmt.Sprintf("%s/v0.1/pipeline_stats", agentURL.String())
	return &httpTransport{
		url:     url,
		client:  client,
		headers: defaultHeaders,
	}
}

func (t *httpTransport) sendPipelineStats(p *StatsPayload) error {
	var buf bytes.Buffer
	gzipWriter, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return err
	}
	if err := msgp.Encode(gzipWriter, p); err != nil {
		return err
	}
	err = gzipWriter.Close()
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", t.url, &buf)
	if err != nil {
		return err
	}
	for header, value := range t.headers {
		req.Header.Set(header, value)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	defer io.Copy(io.Discard, req.Body)
	if code := resp.StatusCode; code >= 400 {
		// error, check the body for context information and
		// return a nice error.
		txt := http.StatusText(code)
		msg := make([]byte, 100)
		n, _ := resp.Body.Read(msg)
		if n > 0 {
			return fmt.Errorf("%s (Status: %s)", msg[:n], txt)
		}
		return fmt.Errorf("%s", txt)
	}
	return nil
}
