package otel

import otelMetric "go.opentelemetry.io/otel/metric"

type allMeters struct {
	client clientMeters
	http   httpMeters
	parse  parseMeters
}

type clientMeters struct {
	total    otelMetric.Int64Counter
	inFlight otelMetric.Int64UpDownCounter
	success  otelMetric.Int64Counter
	failure  otelMetric.Int64Counter
	duration otelMetric.Int64Histogram
}

type httpMeters struct {
	total    otelMetric.Int64Counter
	inFlight otelMetric.Int64UpDownCounter
	success  otelMetric.Int64Counter
	failure  otelMetric.Int64Counter
	redirect otelMetric.Int64Counter
	duration otelMetric.Int64Histogram
	// Failure types
	error            otelMetric.Int64Counter
	errorCode        otelMetric.Int64Counter
	timeout          otelMetric.Int64Counter
	cancelled        otelMetric.Int64Counter
	deadlineExceeded otelMetric.Int64Counter
}

type parseMeters struct {
	total    otelMetric.Int64Counter
	success  otelMetric.Int64Counter
	failure  otelMetric.Int64Counter
	duration otelMetric.Int64Histogram
}

func newMeters(meter otelMetric.Meter) *allMeters {
	return &allMeters{
		client: clientMeters{
			total:    counter(meter, clientPrefix+"request.total", "HTTP client: total requests."),
			inFlight: upDownCounter(meter, clientPrefix+"request.in_flight", "HTTP client: in flight requests."),
			success:  counter(meter, clientPrefix+"request.success", "HTTP client: successful requests."),
			failure:  counter(meter, clientPrefix+"request.failure", "HTTP client: failed requests."),
			duration: histogram(meter, clientPrefix+"request.duration", "HTTP client: requests duration.", "ms"),
		},
		http: httpMeters{
			total:            counter(meter, httpPrefix+"request.total", "HTTP request: total requests."),
			inFlight:         upDownCounter(meter, httpPrefix+"request.in_flight", "HTTP request: in flight requests."),
			success:          counter(meter, httpPrefix+"request.success", "HTTP request: successful requests."),
			failure:          counter(meter, httpPrefix+"request.failure", "HTTP request: failed requests."),
			redirect:         counter(meter, httpPrefix+"request.redirect", "HTTP request: redirects."),
			duration:         histogram(meter, httpPrefix+"request.duration", "HTTP request: response received duration (without parsing).", "ms"),
			error:            counter(meter, httpPrefix+"request.error", "HTTP request: requests with an error (for example a network error)."),
			errorCode:        counter(meter, httpPrefix+"request.errorCode", "HTTP request: requests with HTTP code >= 400."),
			timeout:          counter(meter, httpPrefix+"request.timeout", "HTTP request: timeout requests."),
			cancelled:        counter(meter, httpPrefix+"request.cancelled", "HTTP request: cancelled requests."),
			deadlineExceeded: counter(meter, httpPrefix+"request.deadline_exceeded", "HTTP request: deadline exceeded requests ."),
		},
		parse: parseMeters{
			total:    counter(meter, clientPrefix+"request.parse.total", "HTTP client: total request parse operations."),
			success:  counter(meter, clientPrefix+"request.parse.success", "HTTP client: successful request parse operations."),
			failure:  counter(meter, clientPrefix+"request.parse.failure", "HTTP client: operations request parse failed."),
			duration: histogram(meter, clientPrefix+"request.parse.duration", "HTTP client: request parse duration.", "ms"),
		},
	}
}

func counter(meter otelMetric.Meter, name, desc string) otelMetric.Int64Counter {
	return mustInstrument(meter.Int64Counter(name, otelMetric.WithDescription(desc)))
}

func upDownCounter(meter otelMetric.Meter, name, desc string) otelMetric.Int64UpDownCounter {
	return mustInstrument(meter.Int64UpDownCounter(name, otelMetric.WithDescription(desc)))
}

func histogram(meter otelMetric.Meter, name, desc string, unit string) otelMetric.Int64Histogram {
	return mustInstrument(meter.Int64Histogram(name, otelMetric.WithDescription(desc), otelMetric.WithUnit(unit)))
}

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
