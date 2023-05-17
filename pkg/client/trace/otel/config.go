package otel

import (
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

type config struct {
	propagators         propagation.TextMapPropagator
	redactedPathParams  map[string]struct{}
	redactedQueryParams map[string]struct{}
	redactedHeaders     map[string]struct{}
}

type Option func(*config)

func WithPropagators(v propagation.TextMapPropagator) Option {
	return func(c *config) {
		c.propagators = v
	}
}

func WithRedactedPathParam(params ...string) Option {
	return func(c *config) {
		for _, p := range params {
			c.redactedPathParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

func WithRedactedQueryParam(params ...string) Option {
	return func(c *config) {
		for _, p := range params {
			c.redactedQueryParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

func WithRedactedHeaders(headers ...string) Option {
	return func(c *config) {
		for _, h := range headers {
			c.redactedHeaders[strings.ToLower(h)] = struct{}{}
		}
	}
}

func newConfig(opts []Option) config {

	cfg := config{
		redactedPathParams:  make(map[string]struct{}),
		redactedQueryParams: make(map[string]struct{}),
		// Same as in the otelhttptrace
		redactedHeaders: map[string]struct{}{
			"authorization":       {},
			"www-authenticate":    {},
			"proxy-authenticate":  {},
			"proxy-authorization": {},
			"cookie":              {},
			"set-cookie":          {},
		},
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
