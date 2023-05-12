package keboola

import (
	"github.com/keboola/go-client/pkg/client"
)

type apiConfig struct {
	client         *client.Client
	token          string
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

func WithToken(token string) APIOption {
	return func(c *apiConfig) {
		c.token = token
	}
}
