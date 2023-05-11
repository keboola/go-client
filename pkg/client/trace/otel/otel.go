// Package otel provides OpenTelemetry tracing and metrics for HTTP client requests.
//
// The package provides 3 types of telemetry:
// 1. [otelhttptrace] low-level telemetry:
//   - It provides spans for HTTP request parts, for example: "http.dns", "http.tls", "http.getconn".
//   - Span names start with "http".
//   - Metrics are not provided.
//
// 2. Low-level telemetry
//   - It provides span and metrics for every sent HTTP request, including redirects and retries.
//   - Span name is "http.request".
//   - Metrics names start with "http" (httpPrefix const).
//   - For full list of metrics see the httpMeters struct.
//   - The package [otelhttp] (its client part) is not used, because it doesn't provide metrics.
//
// 3. High-level telemetry implemented in this package.
//   - It provides span and metrics for each "logical" HTTP request send by the client.
//   - Main span "keboola.go.http.client.request" wraps all redirects and retries together.
//   - Span "keboola.go.http.client.request.body.parse" tracks response receiving and parsing (as a stream).
//   - Span "keboola.go.http.client.retry.delay" tracks delay before retry.
//   - Metrics names start with "keboola.go.http.client" (clientPrefix const).
//   - For full list of metrics see the clientMeters and parseMeters structs.
//
// [otelhttp]: https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
// [otelhttptrace]: https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace
package otel

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelMetric "go.opentelemetry.io/otel/metric"
	metricNoop "go.opentelemetry.io/otel/metric/noop"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client/trace"
	"github.com/keboola/go-client/pkg/request"
)

const (
	traceAppName = "github.com/keboola/go-client"
	// httpPrefix: low-level span and metrics, for each redirect and retry.
	httpPrefix          = "http."
	httpRequestSpanName = httpPrefix + "request"
	// clientPrefix: high-level span and metrics.
	clientPrefix             = "keboola.go.http.client."
	clientRequestSpanName    = clientPrefix + "request"
	clientBodyParseSpanName  = httpPrefix + "request.body.parse"
	clientRetryDelaySpanName = clientPrefix + "retry.delay"
)

func NewTrace(tracerProvider otelTrace.TracerProvider, meterProvider otelMetric.MeterProvider, opts ...Option) trace.Factory {
	cfg := newConfig(opts)
	if tracerProvider == nil {
		tracerProvider = otelTrace.NewNoopTracerProvider()
	}
	if meterProvider == nil {
		meterProvider = metricNoop.NewMeterProvider()
	}
	tracer := tracerProvider.Tracer(traceAppName)
	meters := newMeters(meterProvider.Meter(traceAppName))

	return func(rootCtx context.Context, reqDef request.HTTPRequest) (context.Context, *trace.ClientTrace) {
		tc := &trace.ClientTrace{}
		attrs := newAttributes(cfg, reqDef)
		var retryDelaySpan otelTrace.Span

		// Prepare options for low-level tracing created in HTTPRequestStart hook
		clientTraceOpts := []otelhttptrace.ClientTraceOption{otelhttptrace.WithTracerProvider(tracerProvider)}
		for k := range cfg.redactedHeaders {
			clientTraceOpts = append(clientTraceOpts, otelhttptrace.WithRedactedHeaders(k))
		}

		// Create root span and metrics, it may contain multiple HTTP requests (redirects, retries, ...).
		{
			var rootSpan otelTrace.Span

			// Metrics
			startTime := time.Now()
			meters.client.inFlight.Add(rootCtx, 1, otelMetric.WithAttributes(attrs.definition...))

			// Tracing
			rootCtx, rootSpan = tracer.Start(
				rootCtx,
				clientRequestSpanName,
				otelTrace.WithSpanKind(otelTrace.SpanKindClient),
				otelTrace.WithAttributes(attrs.definition...),
				otelTrace.WithAttributes(attrs.definitionExtra...),
			)
			tc.RequestProcessed = func(result any, err error) {
				elapsedTime := float64(time.Since(startTime)) / float64(time.Millisecond)

				// Metrics
				meterAttrs := append(attrs.definition, attrs.httpResponse...)
				meters.client.inFlight.Add(rootCtx, -1, otelMetric.WithAttributes(attrs.definition...)) // same attributes/dimensions as above (+1)!
				meters.client.duration.Record(rootCtx, elapsedTime, otelMetric.WithAttributes(meterAttrs...))

				// Tracing
				rootSpan.SetAttributes(attrs.httpResponse...)      // add attributes from the last response
				rootSpan.SetAttributes(attrs.httpResponseExtra...) // add attributes from the last response
				if retryDelaySpan != nil {
					retryDelaySpan.End()
					retryDelaySpan = nil
				}
				if err == nil {
					rootSpan.End()
				} else {
					rootSpan.RecordError(err)
					rootSpan.SetStatus(codes.Error, err.Error())
					rootSpan.End(otelTrace.WithStackTrace(true))
				}
			}
		}

		// Handle HTTP requests
		{
			var httpRequestStart time.Time
			var httpRequestSpan otelTrace.Span
			tc.HTTPRequestStart = func(req *http.Request) {
				// Metrics
				httpRequestStart = time.Now()
				attrs.SetFromRequest(req)
				meters.http.inFlight.Add(rootCtx, 1, otelMetric.WithAttributes(attrs.httpRequest...))

				// Tracing
				if retryDelaySpan != nil {
					retryDelaySpan.End()
					retryDelaySpan = nil
				}
				var ctx context.Context
				ctx, httpRequestSpan = tracer.Start(
					rootCtx,
					httpRequestSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(attrs.httpRequest...),
					otelTrace.WithAttributes(attrs.httpRequestExtra...),
				)
				// Register low-level tracing under HTTP request span.
				// Use a pkg for Go native - low-level tracing (connect, TLS handshake, dns, ...)
				tc.ClientTrace = *otelhttptrace.NewClientTrace(ctx, clientTraceOpts...)
			}
			tc.HTTPRequestDone = func(res *http.Response, err error) {
				elapsedTime := float64(time.Since(httpRequestStart)) / float64(time.Millisecond)
				attrs.SetFromResponse(res, err)

				// Metrics
				meters.http.inFlight.Add(rootCtx, -1, otelMetric.WithAttributes(attrs.httpRequest...)) // same attributes/dimensions as in HTTPRequestStart!
				meters.http.duration.Record(
					rootCtx,
					elapsedTime,
					otelMetric.WithAttributes(attrs.httpRequest...),
					otelMetric.WithAttributes(attrs.httpResponse...),
					otelMetric.WithAttributes(attrs.httpResponseError...),
				)

				// Tracing
				httpRequestSpan.SetAttributes(attrs.httpResponse...)
				httpRequestSpan.SetAttributes(attrs.httpResponseExtra...)
				switch {
				case err != nil:
					httpRequestSpan.RecordError(err)
					httpRequestSpan.SetStatus(codes.Error, err.Error())
					httpRequestSpan.End(otelTrace.WithStackTrace(true))
				case res != nil && !isSuccess(res, nil):
					httpErr := fmt.Errorf(`HTTP status code %d`, res.StatusCode)
					httpRequestSpan.RecordError(httpErr)
					httpRequestSpan.SetStatus(codes.Error, httpErr.Error())
					httpRequestSpan.End()
				default:
					httpRequestSpan.End()
				}
			}
		}

		// Handle body parsing
		{
			var bodyParseStart time.Time
			var bodyParseSpan otelTrace.Span
			var bodyParseMeterAttrs []attribute.KeyValue
			tc.BodyParseStart = func(response *http.Response) {
				bodyParseStart = time.Now()
				bodyParseMeterAttrs = append(attrs.definition, attrs.httpResponse...)

				// Meters
				meters.parse.inFlight.Add(rootCtx, 1, otelMetric.WithAttributes(bodyParseMeterAttrs...))

				// Tracing
				_, bodyParseSpan = tracer.Start(
					rootCtx,
					clientBodyParseSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(attrs.definition...),
					otelTrace.WithAttributes(attrs.httpRequest...),
					otelTrace.WithAttributes(attrs.httpResponse...),
				)
			}
			tc.BodyParseDone = func(response *http.Response, result any, err error, parseError error) {
				elapsedTime := float64(time.Since(bodyParseStart)) / float64(time.Millisecond)

				// Metrics
				meters.parse.inFlight.Add(rootCtx, -1, otelMetric.WithAttributes(bodyParseMeterAttrs...))
				meters.parse.duration.Record(rootCtx, elapsedTime, otelMetric.WithAttributes(bodyParseMeterAttrs...))

				// Tracing
				if parseError == nil {
					bodyParseSpan.End()
				} else {
					bodyParseSpan.RecordError(parseError)
					bodyParseSpan.SetStatus(codes.Error, parseError.Error())
					bodyParseSpan.End(otelTrace.WithStackTrace(true))
				}
			}
		}

		// Handle retry
		tc.RetryDelay = func(attempt int, delay time.Duration) {
			// retryDelaySpan is ended by HTTPRequestStart hook or RequestProcessed hook (if an error occurred, e.g., request timeout).
			_, retryDelaySpan = tracer.Start(
				rootCtx,
				clientRetryDelaySpanName,
				otelTrace.WithSpanKind(otelTrace.SpanKindClient),
				otelTrace.WithAttributes(attrs.httpRequest...),
				otelTrace.WithAttributes(attrs.httpResponse...),
				otelTrace.WithAttributes(
					attribute.Int("api.request.retry.attempt", attempt),
					attribute.Int64("api.request.retry.delay_ms", delay.Milliseconds()),
					attribute.String("api.request.retry.delay_string", delay.String()),
				),
			)
		}

		return rootCtx, tc
	}
}
