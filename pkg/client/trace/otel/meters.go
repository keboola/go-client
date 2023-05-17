package otel

import otelMetric "go.opentelemetry.io/otel/metric"

const (
	// Low-level metrics, for each redirect and retry.
	httpMeterPrefix = "keboola.go.http."
	// High level metrics.
	clientMeterPrefix = "keboola.go.client."
)

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
			inFlight: upDownCounter(meter, clientMeterPrefix+"request.in_flight", "HTTP client: in flight requests."),
			duration: histogram(meter, clientMeterPrefix+"request.duration", "HTTP client: requests duration.", "ms"),
		},
		http: httpMeters{
			inFlight: upDownCounter(meter, httpMeterPrefix+"request.in_flight", "HTTP request: in flight requests."),
			duration: histogram(meter, httpMeterPrefix+"request.duration", "HTTP request: response received duration (without parsing).", "ms"),
		},
		parse: parseMeters{
			inFlight: upDownCounter(meter, clientMeterPrefix+"request.parse.in_flight", "HTTP client: in flight request parsing."),
			duration: histogram(meter, clientMeterPrefix+"request.parse.duration", "HTTP client: request parse duration.", "ms"),
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
