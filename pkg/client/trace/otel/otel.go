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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelMetric "go.opentelemetry.io/otel/metric"
	metricNoop "go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client/trace"
	"github.com/keboola/go-client/pkg/request"
)

const (
	traceAppName = "github.com/keboola/go-client"
	// HttpPrefix: low-level span and metrics, for each redirect and retry.
	httpPrefix                 = "http."
	httpRequestSpanName        = httpPrefix + "request"
	httpDNSSpanName            = httpPrefix + "dns"
	httpGetConnSpanName        = httpPrefix + "getconn"
	httpConnectSpanName        = httpPrefix + "connect"
	httpTLSHandshakeSpanName   = httpPrefix + "tls"
	httpHeadersSpanName        = httpPrefix + "headers"
	httpSendSpanName           = httpPrefix + "send"
	httpReceiveSpanName        = httpPrefix + "receive"
	attrDNSAddresses           = attribute.Key("http.dns.addrs")
	attrRemoteAddr             = attribute.Key("http.remote")
	attrLocalAddr              = attribute.Key("http.local")
	attrConnectionReused       = attribute.Key("http.conn.reused")
	attrConnectionWasIdle      = attribute.Key("http.conn.wasidle")
	attrConnectionIdleTime     = attribute.Key("http.conn.idletime")
	attrConnectionStartNetwork = attribute.Key("http.conn.start.network")
	attrConnectionDoneNetwork  = attribute.Key("http.conn.done.network")
	attrConnectionDoneAddr     = attribute.Key("http.conn.done.addr")
	attrReadBytes              = attribute.Key("http.read_bytes")
	// ClientPrefix: high-level span and metrics.
	clientPrefix             = "keboola.go.http.client."
	clientRequestSpanName    = clientPrefix + "request"
	clientBodyParseSpanName  = httpPrefix + "request.body.parse"
	clientRetryDelaySpanName = clientPrefix + "retry.delay"
	// Extra attributes for DataDog.
	attrSpanKind            = attribute.Key("span.kind")
	attrSpanKindValueClient = "client"
	attrSpanType            = attribute.Key("span.type")
	attrSpanTypeValueHTTP   = "http"
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

		// Prepare options for low-level tracing created in HTTPRequest hook
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
				otelTrace.WithAttributes(
					attrSpanKind.String(attrSpanKindValueClient),
					attrSpanType.String(attrSpanTypeValueHTTP),
				),
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
		var httpCtx context.Context
		var bodyBytes int64
		{
			var httpRequestStart time.Time
			var httpRequestSpan otelTrace.Span
			var receiveSpan otelTrace.Span
			tc.HTTPRequestStart = func(req *http.Request) {
				bodyBytes = 0

				// End retry delay span
				if retryDelaySpan != nil {
					retryDelaySpan.End()
					retryDelaySpan = nil
				}

				// Create HTTP request span
				httpCtx, httpRequestSpan = tracer.Start(
					rootCtx,
					httpRequestSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(
						attrSpanKind.String(attrSpanKindValueClient),
						attrSpanType.String(attrSpanTypeValueHTTP),
					),
				)

				// Metrics
				httpRequestStart = time.Now()
				attrs.SetFromRequest(req)
				meters.http.inFlight.Add(rootCtx, 1, otelMetric.WithAttributes(attrs.httpRequest...))

				// Tracing
				if retryDelaySpan != nil {
					retryDelaySpan.End()
					retryDelaySpan = nil
				}
				httpCtx, httpRequestSpan = tracer.Start(
					rootCtx,
					httpRequestSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(attrs.httpRequest...),
					otelTrace.WithAttributes(attrs.httpRequestExtra...),
				)
			}
			tc.HTTPResponse = func(res *http.Response, err error) {
				attrs.SetFromResponse(res, err)
				httpRequestSpan.SetAttributes(attrs.httpResponse...)
				httpRequestSpan.SetAttributes(attrs.httpResponseExtra...)
				if res != nil && res.Body != nil && res.Body != http.NoBody {
					_, receiveSpan = tracer.Start(
						httpCtx,
						httpReceiveSpanName,
						otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					)
				}
			}
			tc.HTTPRequestDone = func(res *http.Response, read int64, err error) {
				bodyBytes = read
				elapsedTime := float64(time.Since(httpRequestStart)) / float64(time.Millisecond)

				// End receive span, if any
				if receiveSpan != nil {
					httpRequestSpan.SetAttributes(attrReadBytes.Int64(read))
					receiveSpan.SetAttributes(attrReadBytes.Int64(read))
					if err != nil {
						receiveSpan.RecordError(err)
						receiveSpan.SetStatus(codes.Error, err.Error())
					}
					receiveSpan.End()
				}

				// Metrics
				meters.http.inFlight.Add(rootCtx, -1, otelMetric.WithAttributes(attrs.httpRequest...)) // same attributes/dimensions as in HTTPRequest!
				meters.http.duration.Record(
					rootCtx,
					elapsedTime,
					otelMetric.WithAttributes(attrs.httpRequest...),
					otelMetric.WithAttributes(attrs.httpResponse...),
					otelMetric.WithAttributes(attrs.httpResponseError...),
				)

				// Tracing
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
				bodyParseMeterAttrs = append(bodyParseMeterAttrs, attrs.definition...)
				bodyParseMeterAttrs = append(bodyParseMeterAttrs, attrs.httpResponse...)

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
				bodyParseSpan.SetAttributes(attrReadBytes.Int64(bodyBytes))
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
			// retryDelaySpan is ended by HTTPRequest hook or RequestProcessed hook (if an error occurred, e.g., request timeout).
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

		// Register low-level tracing.
		// "otelhttptrace" pkg from the opentelemetry-contrib module is buggy, does not end spans:
		// https://github.com/open-telemetry/opentelemetry-go-contrib/issues/399
		// httptrace: DNS
		{
			var dnsSpan otelTrace.Span
			tc.DNSStart = func(info httptrace.DNSStartInfo) {
				_, dnsSpan = tracer.Start(
					httpCtx,
					httpDNSSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(semconv.NetHostName(info.Host)),
				)
			}
			tc.DNSDone = func(info httptrace.DNSDoneInfo) {
				var addrs []string
				for _, netAddr := range info.Addrs {
					addrs = append(addrs, netAddr.String())
				}
				dnsSpan.SetAttributes(attrDNSAddresses.String(strings.Join(addrs, ";")))
				if info.Err != nil {
					dnsSpan.RecordError(info.Err)
					dnsSpan.SetStatus(codes.Error, info.Err.Error())
				}
				dnsSpan.End()
			}
		}
		// httptrace: Get connection
		{
			var getConnSpan otelTrace.Span
			tc.GetConn = func(host string) {
				_, getConnSpan = tracer.Start(
					httpCtx,
					httpGetConnSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(semconv.NetHostName(host)),
				)
			}
			tc.GotConn = func(info httptrace.GotConnInfo) {
				getConnSpan.SetAttributes(
					attrRemoteAddr.String(info.Conn.RemoteAddr().String()),
					attrLocalAddr.String(info.Conn.LocalAddr().String()),
					attrConnectionReused.Bool(info.Reused),
					attrConnectionWasIdle.Bool(info.WasIdle),
				)
				if info.WasIdle {
					getConnSpan.SetAttributes(attrConnectionIdleTime.String(info.IdleTime.String()))
				}
				getConnSpan.End()
			}
		}
		// httptrace: Connect
		{
			var connectSpan otelTrace.Span
			tc.ConnectStart = func(network, addr string) {
				_, connectSpan = tracer.Start(
					httpCtx,
					httpConnectSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					otelTrace.WithAttributes(
						attrRemoteAddr.String(addr),
						attrConnectionStartNetwork.String(network),
					),
				)
			}
			tc.ConnectDone = func(network, addr string, err error) {
				connectSpan.SetAttributes(
					attrConnectionDoneAddr.String(addr),
					attrConnectionDoneNetwork.String(network),
				)
				if err != nil {
					connectSpan.RecordError(err)
					connectSpan.SetStatus(codes.Error, err.Error())
				}
				connectSpan.End()
			}
		}
		// httptrace: TLS
		{
			var tlsSpan otelTrace.Span
			tc.TLSHandshakeStart = func() {
				_, tlsSpan = tracer.Start(
					httpCtx,
					httpTLSHandshakeSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
				)
			}
			tc.TLSHandshakeDone = func(_ tls.ConnectionState, err error) {
				if err != nil {
					tlsSpan.RecordError(err)
					tlsSpan.SetStatus(codes.Error, err.Error())
				}
				tlsSpan.End()
			}
		}
		{
			var headersSpan otelTrace.Span
			var sendSpan otelTrace.Span
			tc.WroteHeaderField = func(_ string, _ []string) {
				// Start headers span at first header
				if headersSpan == nil {
					_, headersSpan = tracer.Start(
						httpCtx,
						httpHeadersSpanName,
						otelTrace.WithSpanKind(otelTrace.SpanKindClient),
					)
				}
			}
			tc.WroteHeaders = func() {
				// End headers span, if any
				if headersSpan == nil {
					headersSpan.End()
					headersSpan = nil
				}

				// Start send span
				_, sendSpan = tracer.Start(
					httpCtx,
					httpSendSpanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindClient),
				)
			}
			tc.WroteRequest = func(info httptrace.WroteRequestInfo) {
				// End send span
				if info.Err != nil {
					sendSpan.RecordError(info.Err)
					sendSpan.SetStatus(codes.Error, info.Err.Error())
				}
				sendSpan.End()
			}
		}

		return rootCtx, tc
	}
}
