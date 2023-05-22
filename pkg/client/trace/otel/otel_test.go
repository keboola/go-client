package otel_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	export "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/client/trace/otel"
	"github.com/keboola/go-client/pkg/request"
)

const (
	testTraceID      = 0xabcd
	testSpanIDBase   = 0x1000
	testRequestBody  = "my-body"
	testResponseBody = "OK"
)

func TestSimpleRealRequest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup tracing
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := trace.NewTracerProvider(trace.WithSyncer(traceExporter), trace.WithIDGenerator(&testIDGenerator{}))

	// Setup metrics
	metricExporter, err := export.New()
	assert.NoError(t, err)
	meterProvider := metric.NewMeterProvider(metric.WithReader(metricExporter))

	// Create client
	c := client.New().
		WithTransport(client.DefaultTransport()).
		WithRetry(client.RetryConfig{
			Condition:     client.DefaultRetryCondition(),
			Count:         3,
			WaitTimeStart: 1 * time.Millisecond,
			WaitTimeMax:   20 * time.Millisecond,
		}).
		WithTelemetry(
			tracerProvider,
			meterProvider,
			otel.WithRedactedPathParam("secret1"),
			otel.WithRedactedQueryParam("secret2"),
			otel.WithRedactedHeaders("X-StorageAPI-Token"),
			otel.WithPropagators(propagation.TraceContext{}),
		)

	// Run request
	str := ""
	httpRequest := request.NewHTTPRequest(c).
		WithGet("https://www.jsontest.com").
		WithResult(&str)
	apiRequest := request.NewAPIRequest(&str, httpRequest)
	result, err := apiRequest.Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, *result)

	// Assert spans
	spans := traceExporter.GetSpans()
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].SpanContext.SpanID().String() < spans[j].SpanContext.SpanID().String()
	})
	var spanNames []string
	for _, span := range spans {
		spanNames = append(spanNames, span.Name)
		// All spans must be finished!
		assert.NotZero(t, span.StartTime)
		assert.NotZero(t, span.EndTime)
	}
	assert.Equal(t, []string{
		"keboola.go.api.client.request",
		"keboola.go.client.request",
		"http.request",
		"http.getconn",
		"http.dns",
		"http.connect",
		"http.tls",
		"http.headers",
		"http.send",
		"http.receive",
		"http.request.body.parse",
	}, spanNames)

	// Assert metrics
	metricsAll := &metricdata.ResourceMetrics{}
	assert.NoError(t, metricExporter.Collect(ctx, metricsAll))
	assert.Len(t, metricsAll.ScopeMetrics, 1)
	metrics := metricsAll.ScopeMetrics[0].Metrics
	sort.SliceStable(metrics, func(i, j int) bool {
		return metrics[i].Name < metrics[j].Name
	})
	var metricsNames []string
	for _, m := range metrics {
		metricsNames = append(metricsNames, m.Name)
	}
	assert.Equal(t, []string{
		"keboola.go.client.request.duration",
		"keboola.go.client.request.in_flight",
		"keboola.go.client.request.parse.duration",
		"keboola.go.client.request.parse.in_flight",
		"keboola.go.http.request.content_length",
		"keboola.go.http.request.duration",
		"keboola.go.http.request.in_flight",
		"keboola.go.http.response.content_length",
	}, metricsNames)
}

func TestComplexMockedRequest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mocked responses (2x redirect, 3x retry, OK)
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("POST", `https://connection.keboola.com/my-secret/redirect1`, func(request *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Location", "https://connection.keboola.com/redirect2")
		body, err := io.ReadAll(request.Body)
		assert.NoError(t, err)
		assert.Equal(t, testRequestBody, string(body))
		return &http.Response{
			StatusCode: http.StatusMovedPermanently,
			Header:     header,
		}, nil
	})
	transport.RegisterResponder("GET", `https://connection.keboola.com/redirect2`, func(request *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Location", "https://connection.keboola.com/index")
		return &http.Response{
			StatusCode: http.StatusMovedPermanently,
			Header:     header,
		}, nil
	})
	attempt := 0
	transport.RegisterResponder("GET", `https://connection.keboola.com/index`, func(h *http.Request) (*http.Response, error) {
		attempt++
		switch attempt {
		case 1:
			return nil, &net.DNSError{Err: "some network error", IsTemporary: true}
		case 2:
			return &http.Response{StatusCode: http.StatusLocked}, nil
		case 3:
			return &http.Response{StatusCode: http.StatusTooManyRequests}, nil
		case 4:
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(testResponseBody))}, nil
		default:
			panic(fmt.Errorf(`unexpected attempt "%d"`, attempt))
		}
	})

	// Setup tracing
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := trace.NewTracerProvider(trace.WithSyncer(traceExporter), trace.WithIDGenerator(&testIDGenerator{}))

	// Setup metrics
	metricExporter, err := export.New()
	assert.NoError(t, err)
	meterProvider := metric.NewMeterProvider(metric.WithReader(metricExporter))

	// Create client
	c := client.New().
		WithTransport(transport).
		WithBaseURL("https://connection.keboola.com").
		WithRetry(client.RetryConfig{
			Condition:     client.DefaultRetryCondition(),
			Count:         3,
			WaitTimeStart: 1 * time.Millisecond,
			WaitTimeMax:   20 * time.Millisecond,
		}).
		WithTelemetry(
			tracerProvider,
			meterProvider,
			otel.WithRedactedPathParam("secret1"),
			otel.WithRedactedQueryParam("secret2"),
			otel.WithRedactedHeaders("X-StorageAPI-Token"),
			otel.WithPropagators(propagation.TraceContext{}),
		)

	// Run request
	str := ""
	httpRequest := request.NewHTTPRequest(c).
		WithPost("/{secret1}/redirect1").
		WithBody(testRequestBody).
		AndPathParam("secret1", "my-secret").
		AndQueryParam("foo", "bar").
		AndQueryParam("secret2", "my-secret").
		AndHeader("X-StorageAPI-Token", "my-secret").
		WithResult(&str)
	apiRequest := request.NewAPIRequest(&str, httpRequest)
	result, err := apiRequest.Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, testResponseBody, *result)

	// Get expected and actual spans and metrics
	expectedSpans := getExpectedSpans()
	actualSpans := getActualSpans(t, traceExporter)
	expectedMetrics := getExpectedMetrics()
	actualMetrics := getActualMetrics(t, ctx, metricExporter)

	// Compare spans one by one, for easier debugging
	assert.Equalf(
		t, len(expectedSpans), len(actualSpans),
		`unexpected number of spans: actual "%d", expected "%d"`, len(actualSpans), len(expectedSpans),
	)
	spansCount := (int)(math.Max((float64)(len(expectedSpans)), (float64)(len(expectedSpans))))
	var actualSpan tracetest.SpanStub
	var expectedSpan tracetest.SpanStub
	for i := 0; i < spansCount; i++ {
		if len(actualSpans) > i {
			actualSpan = actualSpans[i]
		} else {
			actualSpan = tracetest.SpanStub{Name: "<missing span>"}
		}
		if len(expectedSpans) > i {
			expectedSpan = expectedSpans[i]
		} else {
			expectedSpan = tracetest.SpanStub{Name: "<missing span>"}
		}
		if assert.Equalf(t, expectedSpan.Name, actualSpan.Name, `span position in list "%d"`, i+1) {
			assert.Equal(t, expectedSpan, actualSpan)
		}
	}

	// Compare metrics one by one, for easier debugging
	assert.Equalf(
		t, len(expectedMetrics), len(actualMetrics),
		`unexpected number of metrics: actual "%d", expected "%d"`, len(expectedMetrics), len(actualMetrics),
	)
	metersCount := (int)(math.Max((float64)(len(expectedMetrics)), (float64)(len(actualMetrics))))
	var actualMeter metricdata.Metrics
	var expectedMeter metricdata.Metrics
	for i := 0; i < metersCount; i++ {
		if len(actualMetrics) > i {
			actualMeter = actualMetrics[i]
		} else {
			actualMeter = metricdata.Metrics{Name: "<missing metric>"}
		}
		if len(expectedMetrics) > i {
			expectedMeter = expectedMetrics[i]
		} else {
			expectedMeter = metricdata.Metrics{Name: "<missing metric>"}
		}
		if assert.Equalf(t, expectedMeter.Name, actualMeter.Name, `meter position in list "%d"`, i+1) {
			assert.Equal(t, expectedMeter, actualMeter)
		}
	}
}

func getActualSpans(t *testing.T, exporter *tracetest.InMemoryExporter) tracetest.SpanStubs {
	t.Helper()
	spans := exporter.GetSpans()
	cleanAndSortSpans(spans)
	return spans
}

func cleanAndSortSpans(spans tracetest.SpanStubs) {
	// Sort spans
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].SpanContext.SpanID().String() < spans[j].SpanContext.SpanID().String()
	})

	// Clean dynamic values
	for i := range spans {
		span := &spans[i]
		span.StartTime = time.Time{}
		span.EndTime = time.Time{}
		span.Resource = nil
		span.InstrumentationLibrary.Name = ""
		for j := range span.Events {
			event := &span.Events[j]
			event.Time = time.Time{}
		}
	}
}

func getActualMetrics(t *testing.T, ctx context.Context, reader metric.Reader) []metricdata.Metrics {
	t.Helper()
	all := &metricdata.ResourceMetrics{}
	assert.NoError(t, reader.Collect(ctx, all))
	assert.Len(t, all.ScopeMetrics, 1)
	metrics := all.ScopeMetrics[0].Metrics
	cleanAndSortMetrics(metrics)
	return metrics
}

func cleanAndSortMetrics(metrics []metricdata.Metrics) {
	// DataPoints have random order, sort them by statusCode and URL.7
	// "0" means a request metric, the status code is not known yet.
	keyOrder := map[string]int{
		"0:https://connection.keboola.com/..../redirect1?foo=....&secret2=....":   1,
		"301:https://connection.keboola.com/..../redirect1?foo=....&secret2=....": 2,
		"0:https://connection.keboola.com/redirect2":                              3,
		"301:https://connection.keboola.com/redirect2":                            4,
		"0:https://connection.keboola.com/index":                                  5,
		"423:https://connection.keboola.com/index":                                6,
		"429:https://connection.keboola.com/index":                                7,
		"200:https://connection.keboola.com/index":                                8,
	}
	dataPointKey := func(attrs attribute.Set) string {
		status, _ := attrs.Value("http.status_code")
		url, _ := attrs.Value("http.url")
		return fmt.Sprintf("%d:%s", status.AsInt64(), url.AsString())
	}
	dataPointOrder := func(attrs attribute.Set) int {
		key := dataPointKey(attrs)
		order, found := keyOrder[key]
		if !found {
			panic(fmt.Errorf(`unexpected request %q"`, key))
		}
		return order
	}

	// Clean dynamic values
	for i := range metrics {
		item := &metrics[i]
		switch record := item.Data.(type) {
		case metricdata.Sum[int64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointOrder(record.DataPoints[i].Attributes) < dataPointOrder(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
			}
		case metricdata.Sum[float64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointOrder(record.DataPoints[i].Attributes) < dataPointOrder(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
			}
		case metricdata.Histogram[int64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointOrder(record.DataPoints[i].Attributes) < dataPointOrder(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.BucketCounts = nil
				point.Min = metricdata.Extrema[int64]{}
				point.Max = metricdata.Extrema[int64]{}
				point.Sum = 0
			}
		case metricdata.Histogram[float64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointOrder(record.DataPoints[i].Attributes) < dataPointOrder(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.BucketCounts = nil
				point.Min = metricdata.Extrema[float64]{}
				point.Max = metricdata.Extrema[float64]{}
				point.Sum = 0
			}
		}
	}
}

func getExpectedSpans() tracetest.SpanStubs {
	// Note: "httptrace" spans are not included, they are not created by the mocked transport.
	apiSpanContext := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    toTraceID(testTraceID),
		SpanID:     toSpanID(testSpanIDBase + 1),
		TraceFlags: otelTrace.FlagsSampled,
	})
	clientReqSpanContext := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    toTraceID(testTraceID),
		SpanID:     toSpanID(testSpanIDBase + 2),
		TraceFlags: otelTrace.FlagsSampled,
	})
	httpReqSpanContext := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    toTraceID(testTraceID),
		SpanID:     toSpanID(testSpanIDBase + 11),
		TraceFlags: otelTrace.FlagsSampled,
	})
	return tracetest.SpanStubs{
		// API request span
		{
			Name:           "keboola.go.api.client.request",
			SpanKind:       otelTrace.SpanKindClient,
			SpanContext:    apiSpanContext,
			ChildSpanCount: 1,
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.Int("api.requests_count", 1),
				attribute.String("http.result_type", "*string"),
				attribute.String("resource.name", "otel_test.TestComplexMockedRequest"),
				attribute.String("api.request_defined_in", "otel_test.TestComplexMockedRequest"),
			},
		},
		// HTTP client request span
		{
			Name:           "keboola.go.client.request",
			SpanKind:       otelTrace.SpanKindClient,
			Parent:         apiSpanContext,
			SpanContext:    clientReqSpanContext,
			ChildSpanCount: 9,
			Attributes: []attribute.KeyValue{
				attribute.String("resource.name", "/{secret1}/redirect1"),
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("http.result_type", "*string"),
				attribute.String("http.method", "POST"),
				attribute.String("http.url", "https://connection.keboola.com/{secret1}/redirect1"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/{secret1}/redirect1"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.X-Storageapi-Token", "****"),
				attribute.String("http.url.path_params.secret1", "****"),
				attribute.String("http.query.foo", "bar"),
				attribute.String("http.query.secret2", "****"),
				attribute.Int("http.status_code", 200),
			},
		},
		// Redirect 1
		{
			Name:     "http.request",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 3),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/..../redirect1"),
				attribute.String("http.method", "POST"),
				attribute.String("http.flavor", "1.1"),
				attribute.String("http.url", "https://connection.keboola.com/..../redirect1?foo=....&secret2=...."),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/..../redirect1"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-1003000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.String("http.query.foo", "bar"),
				attribute.String("http.query.secret2", "****"),
				attribute.Int("http.status_code", 301),
				attribute.Bool("http.is_redirection", true),
				attribute.String("http.response.header.location", "https://connection.keboola.com/redirect2"),
				attribute.Int64("http.wrote_bytes", 7),
				attribute.Int64("http.read_bytes", 0),
			},
		},
		// Redirect 2
		{
			Name:     "http.request",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 4),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/redirect2"),
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/redirect2"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/redirect2"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.referer", "https://connection.keboola.com/my-secret/redirect1?foo=bar&secret2=my-secret"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-1004000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.Int("http.status_code", 301),
				attribute.Bool("http.is_redirection", true),
				attribute.String("http.response.header.location", "https://connection.keboola.com/index"),
				attribute.Int64("http.wrote_bytes", 0),
				attribute.Int64("http.read_bytes", 0),
			},
		},
		// Network Error
		{
			Name:     "http.request",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 5),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Status: trace.Status{
				Code:        codes.Error,
				Description: "lookup : some network error",
			},
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/index"),
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.referer", "https://connection.keboola.com/redirect2"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-1005000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.String("http.error_type", "net"),
				attribute.Int64("http.wrote_bytes", 0),
				attribute.Int64("http.read_bytes", 0),
			},
			Events: []trace.Event{
				{
					Name: "exception",
					Attributes: []attribute.KeyValue{
						attribute.String("exception.type", "*net.DNSError"),
						attribute.String("exception.message", "lookup : some network error"),
					},
				},
			},
		},
		// Retry delay 1
		{
			Name:     "keboola.go.client.retry.delay",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 6),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.error_type", "net"),
				attribute.Int("api.request.retry.attempt", 1),
				attribute.Int("api.request.retry.delay_ms", 1),
				attribute.String("api.request.retry.delay_string", "1ms"),
			},
		},
		// HTTP Error Code 423
		{
			Name:     "http.request",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 7),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Status: trace.Status{
				Code:        codes.Error,
				Description: "HTTP status code: 423 Locked",
			},
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/index"),
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.referer", "https://connection.keboola.com/redirect2"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-1007000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.Int("http.status_code", http.StatusLocked),
				attribute.String("http.error_type", "http_4xx_code"),
				attribute.Int64("http.wrote_bytes", 0),
				attribute.Int64("http.read_bytes", 0),
			},
			Events: []trace.Event{
				{
					Name: "exception",
					Attributes: []attribute.KeyValue{
						attribute.String("exception.type", "*errors.errorString"),
						attribute.String("exception.message", "HTTP status code: 423 Locked"),
					},
				},
			},
		},
		// Retry delay 2
		{
			Name:     "keboola.go.client.retry.delay",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 8),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.Int("http.status_code", 423),
				attribute.String("http.error_type", "http_4xx_code"),
				attribute.Int("api.request.retry.attempt", 2),
				attribute.Int("api.request.retry.delay_ms", 2),
				attribute.String("api.request.retry.delay_string", "2ms"),
			},
		},
		// HTTP Error Code 429
		{
			Name:     "http.request",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 9),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Status: trace.Status{
				Code:        codes.Error,
				Description: "HTTP status code: 429 Too Many Requests",
			},
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/index"),
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.referer", "https://connection.keboola.com/redirect2"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-1009000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.Int("http.status_code", http.StatusTooManyRequests),
				attribute.String("http.error_type", "http_4xx_code"),
				attribute.Int64("http.wrote_bytes", 0),
				attribute.Int64("http.read_bytes", 0),
			},
			Events: []trace.Event{
				{
					Name: "exception",
					Attributes: []attribute.KeyValue{
						attribute.String("exception.type", "*errors.errorString"),
						attribute.String("exception.message", "HTTP status code: 429 Too Many Requests"),
					},
				},
			},
		},
		// Retry delay 3
		{
			Name:     "keboola.go.client.retry.delay",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   clientReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 10),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.Int("http.status_code", 429),
				attribute.String("http.error_type", "http_4xx_code"),
				attribute.Int("api.request.retry.attempt", 3),
				attribute.Int("api.request.retry.delay_ms", 4),
				attribute.String("api.request.retry.delay_string", "4ms"),
			},
		},
		// HTTP OK
		{
			Name:           "http.request",
			SpanKind:       otelTrace.SpanKindClient,
			ChildSpanCount: 1,
			Parent:         clientReqSpanContext,
			SpanContext:    httpReqSpanContext,
			Attributes: []attribute.KeyValue{
				attribute.String("span.kind", "client"),
				attribute.String("span.type", "http"),
				attribute.String("resource.name", "/index"),
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.String("http.header.accept-encoding", "gzip, br"),
				attribute.String("http.header.referer", "https://connection.keboola.com/redirect2"),
				attribute.String("http.header.traceparent", "00-abcd0000000000000000000000000000-100b000000000000-01"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.Int("http.status_code", http.StatusOK),
				attribute.Int64("http.wrote_bytes", 0),
				attribute.Int64("http.read_bytes", 2),
			},
		},
		// Body parse
		{
			Name:     "http.request.body.parse",
			SpanKind: otelTrace.SpanKindClient,
			Parent:   httpReqSpanContext,
			SpanContext: otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
				TraceID:    toTraceID(testTraceID),
				SpanID:     toSpanID(testSpanIDBase + 12),
				TraceFlags: otelTrace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.flavor", ""), // missing because the mocked transport is used
				attribute.String("http.url", "https://connection.keboola.com/index"),
				attribute.String("net.peer.name", "connection.keboola.com"),
				attribute.String("http.user_agent", "keboola-go-client"),
				attribute.String("http.url_details.scheme", "https"),
				attribute.String("http.url_details.path", "/index"),
				attribute.String("http.url_details.host", "connection.keboola.com"),
				attribute.String("http.url_details.host_prefix", "connection"),
				attribute.String("http.url_details.host_suffix", "keboola.com"),
				attribute.Int("http.status_code", http.StatusOK),
				attribute.Int64("http.read_bytes", 2),
			},
		},
	}
}

func getExpectedMetrics() []metricdata.Metrics {
	histBounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000} // ms
	attrsRequestDefinition := attribute.NewSet(
		attribute.String("http.result_type", "*string"),
		attribute.String("http.method", "POST"),
		attribute.String("http.url", "https://connection.keboola.com/{secret1}/redirect1"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/{secret1}/redirect1"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
	)
	attrsRequestDefinitionWithStatus := attribute.NewSet(
		attribute.String("http.result_type", "*string"),
		attribute.String("http.method", "POST"),
		attribute.String("http.url", "https://connection.keboola.com/{secret1}/redirect1"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/{secret1}/redirect1"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 200),
	)
	attrsRedirect1Status301 := attribute.NewSet(
		attribute.String("http.method", "POST"),
		attribute.String("http.flavor", "1.1"),
		attribute.String("http.url", "https://connection.keboola.com/..../redirect1?foo=....&secret2=...."),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/..../redirect1"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 301),
		attribute.Bool("http.is_redirection", true),
	)
	attrsRedirect2Status301 := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/redirect2"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/redirect2"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 301),
		attribute.Bool("http.is_redirection", true),
	)
	attrsIndexNetworkError := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/index"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/index"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.String("http.error_type", "net"),
	)
	attrsIndexStatus423 := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/index"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/index"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 423),
		attribute.String("http.error_type", "http_4xx_code"),
	)
	attrsIndexStatus429 := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/index"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/index"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 429),
		attribute.String("http.error_type", "http_4xx_code"),
	)
	attrsIndexStatus200 := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/index"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/index"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
		attribute.Int("http.status_code", 200),
	)
	attrsInFlightRedirect1 := attribute.NewSet(
		attribute.String("http.method", "POST"),
		attribute.String("http.flavor", "1.1"),
		attribute.String("http.url", "https://connection.keboola.com/..../redirect1?foo=....&secret2=...."),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/..../redirect1"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
	)
	attrsInFlightRedirect2 := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/redirect2"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/redirect2"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
	)
	attrsInFlightIndex := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.flavor", ""), // missing because the mocked transport is used
		attribute.String("http.url", "https://connection.keboola.com/index"),
		attribute.String("net.peer.name", "connection.keboola.com"),
		attribute.String("http.user_agent", "keboola-go-client"),
		attribute.String("http.url_details.scheme", "https"),
		attribute.String("http.url_details.path", "/index"),
		attribute.String("http.url_details.host", "connection.keboola.com"),
		attribute.String("http.url_details.host_prefix", "connection"),
		attribute.String("http.url_details.host_suffix", "keboola.com"),
	)
	return []metricdata.Metrics{
		// High-level metrics keboola.go.client.*
		{
			Name:        "keboola.go.client.request.in_flight",
			Description: "HTTP client: in flight requests.",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: false, // upDownCounter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: attrsRequestDefinition},
				},
			},
		},
		{
			Name:        "keboola.go.client.request.duration",
			Description: "HTTP client: requests duration.",
			Unit:        "ms",
			Data: metricdata.Histogram[float64]{
				Temporality: 1,
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{Count: 1, Bounds: histBounds, Attributes: attrsRequestDefinitionWithStatus},
				},
			},
		},
		// Body parsing metrics keboola.go.client.request.parse.*
		{
			Name:        "keboola.go.client.request.parse.in_flight",
			Description: "HTTP client: in flight request parsing.",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: false, // upDownCounter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: attrsRequestDefinitionWithStatus},
				},
			},
		},
		{
			Name:        "keboola.go.client.request.parse.duration",
			Description: "HTTP client: request parse duration.",
			Unit:        "ms",
			Data: metricdata.Histogram[float64]{
				Temporality: 1,
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{Count: 1, Bounds: histBounds, Attributes: attrsRequestDefinitionWithStatus},
				},
			},
		},
		// Low-level metrics keboola.go.http.request.*
		{
			Name:        "keboola.go.http.request.in_flight",
			Description: "HTTP request: in flight requests.",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: false, // upDownCounter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: attrsInFlightRedirect1},
					{Value: 0, Attributes: attrsInFlightRedirect2},
					{Value: 0, Attributes: attrsInFlightIndex},
				},
			},
		},
		{
			Name:        "keboola.go.http.request.duration",
			Description: "HTTP request: response received duration (without parsing).",
			Unit:        "ms",
			Data: metricdata.Histogram[float64]{
				Temporality: 1,
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{Count: 1, Bounds: histBounds, Attributes: attrsRedirect1Status301},
					{Count: 1, Bounds: histBounds, Attributes: attrsRedirect2Status301},
					{Count: 1, Bounds: histBounds, Attributes: attrsIndexNetworkError},
					{Count: 1, Bounds: histBounds, Attributes: attrsIndexStatus423},
					{Count: 1, Bounds: histBounds, Attributes: attrsIndexStatus429},
					{Count: 1, Bounds: histBounds, Attributes: attrsIndexStatus200},
				},
			},
		},
		// Content-length metrics
		{
			Name:        "keboola.go.http.request.content_length",
			Description: "HTTP request: length of sent content after compression.",
			Unit:        "By",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: int64(len(testRequestBody)), Attributes: attrsRedirect1Status301},
					{Value: 0, Attributes: attrsRedirect2Status301},
					{Value: 0, Attributes: attrsIndexNetworkError},
					{Value: 0, Attributes: attrsIndexStatus423},
					{Value: 0, Attributes: attrsIndexStatus429},
					{Value: 0, Attributes: attrsIndexStatus200},
				},
			},
		},
		{
			Name:        "keboola.go.http.response.content_length",
			Description: "HTTP response: length of received content before decompression.",
			Unit:        "By",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: attrsRedirect1Status301},
					{Value: 0, Attributes: attrsRedirect2Status301},
					{Value: 0, Attributes: attrsIndexNetworkError},
					{Value: 0, Attributes: attrsIndexStatus423},
					{Value: 0, Attributes: attrsIndexStatus429},
					{Value: int64(len(testResponseBody)), Attributes: attrsIndexStatus200},
				},
			},
		},
	}
}

type testIDGenerator struct {
	spanID uint16
}

func (g *testIDGenerator) NewIDs(ctx context.Context) (otelTrace.TraceID, otelTrace.SpanID) {
	traceID := toTraceID(testTraceID)
	return traceID, g.NewSpanID(ctx, traceID)
}

func (g *testIDGenerator) NewSpanID(_ context.Context, _ otelTrace.TraceID) otelTrace.SpanID {
	g.spanID++
	return toSpanID(testSpanIDBase + g.spanID)
}

func toTraceID(in uint16) otelTrace.TraceID { //nolint: unparam
	tmp := make([]byte, 16)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[16]byte)(tmp)
}

func toSpanID(in uint16) otelTrace.SpanID {
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[8]byte)(tmp)
}
