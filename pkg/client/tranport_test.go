package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

func TestDefaultTransport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out := ""
	url := "https://www.google.com"
	c := client.New().WithTransport(client.DefaultTransport()) // <<<<<<<<<
	apiRequest := request.NewAPIRequest(&out, request.NewHTTPRequest(c).WithGet(url).WithResult(&out))
	result, err := apiRequest.Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, *result)
}

func TestHTTP2Transport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out := ""
	url := "https://www.google.com"
	c := client.New().WithTransport(client.HTTP2Transport()) // <<<<<<<<<
	apiRequest := request.NewAPIRequest(&out, request.NewHTTPRequest(c).WithGet(url).WithResult(&out))
	result, err := apiRequest.Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, *result)
}
