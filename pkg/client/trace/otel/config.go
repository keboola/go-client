package otel

import "strings"

type config struct {
	redactedHeaders map[string]struct{}
}

type Option func(*config)

func WithRedactedHeaders(headers ...string) Option {
	return func(c *config) {
		for _, h := range headers {
			c.redactedHeaders[strings.ToLower(h)] = struct{}{}
		}
	}
}

func newConfig(opts []Option) config {
	cfg := config{
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
