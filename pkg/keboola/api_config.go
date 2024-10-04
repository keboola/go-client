package keboola

import (
	"time"

	otelMetric "go.opentelemetry.io/otel/metric"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client"
)

type apiConfig struct {
	client           *client.Client
	onSuccessTimeout time.Duration
	tracerProvider   otelTrace.TracerProvider
	meterProvider    otelMetric.MeterProvider
}

type APIOption func(c *apiConfig)

func newAPIConfig(opts []APIOption) apiConfig {
	cfg := apiConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func WithClient(cl *client.Client) APIOption {
	return func(c *apiConfig) {
		c.client = cl
	}
}

func WithOnSuccessTimeout(timeout time.Duration) APIOption {
	return func(c *apiConfig) {
		c.onSuccessTimeout = timeout
	}
}

func WithTracerProvider(v otelTrace.TracerProvider) APIOption {
	return func(c *apiConfig) {
		c.tracerProvider = v
	}
}

func WithMeterProvider(v otelMetric.MeterProvider) APIOption {
	return func(c *apiConfig) {
		c.meterProvider = v
	}
}
