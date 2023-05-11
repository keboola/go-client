package otel

import otelMetric "go.opentelemetry.io/otel/metric"

type allMeters struct {
	client clientMeters
	http   httpMeters
	parse  parseMeters
}

type clientMeters struct {
	inFlight otelMetric.Int64UpDownCounter
	duration otelMetric.Float64Histogram
}

type httpMeters struct {
	inFlight otelMetric.Int64UpDownCounter
	duration otelMetric.Float64Histogram
}

type parseMeters struct {
	inFlight otelMetric.Int64UpDownCounter
	duration otelMetric.Float64Histogram
}

func newMeters(meter otelMetric.Meter) *allMeters {
	return &allMeters{
		client: clientMeters{
			inFlight: upDownCounter(meter, clientPrefix+"request.in_flight", "HTTP client: in flight requests."),
			duration: histogram(meter, clientPrefix+"request.duration", "HTTP client: requests duration.", "ms"),
		},
		http: httpMeters{
			inFlight: upDownCounter(meter, httpPrefix+"request.in_flight", "HTTP request: in flight requests."),
			duration: histogram(meter, httpPrefix+"request.duration", "HTTP request: response received duration (without parsing).", "ms"),
		},
		parse: parseMeters{
			inFlight: upDownCounter(meter, clientPrefix+"request.parse.in_flight", "HTTP client: in flight request parsing."),
			duration: histogram(meter, clientPrefix+"request.parse.duration", "HTTP client: request parse duration.", "ms"),
		},
	}
}

func upDownCounter(meter otelMetric.Meter, name, desc string) otelMetric.Int64UpDownCounter {
	return mustInstrument(meter.Int64UpDownCounter(name, otelMetric.WithDescription(desc)))
}

func histogram(meter otelMetric.Meter, name, desc string, unit string) otelMetric.Float64Histogram {
	return mustInstrument(meter.Float64Histogram(name, otelMetric.WithDescription(desc), otelMetric.WithUnit(unit)))
}

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
