package keboola_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
)

func TestNewAPI_WithoutIndex(t *testing.T) {
	t.Parallel()

	c, transport := mockedClient()

	ctx := context.Background()
	api := keboola.NewAPI(ctx, "https://connection.keboola.mock", keboola.WithClient(&c))
	assert.Equal(t, keboola.Services{{ID: "queue", URL: "https://queue.keboola.mock"}, {ID: "scheduler", URL: "https://scheduler.keboola.mock"}}, api.Index().Services)
	assert.Equal(t, keboola.Features{"dynamic-backend-size"}, api.Index().Features)

	assert.Equal(t, 1, transport.GetCallCountInfo()["GET /v2/storage/?exclude=components"])
}

func TestNewAPI_WithIndex(t *testing.T) {
	t.Parallel()

	c, transport := mockedClient()

	idx := &keboola.Index{
		Services: keboola.Services{
			{
				ID:  "templates",
				URL: "https://templates.keboola.mock",
			},
		},
		Features: keboola.Features{"project-read-only-role-enabled"},
	}

	ctx := context.Background()
	api := keboola.NewAPI(ctx, "https://connection.keboola.mock", keboola.WithClient(&c), keboola.WithIndex(idx))
	assert.Equal(t, idx.Services, api.Index().Services)
	assert.Equal(t, idx.Features, api.Index().Features)

	// Index request was not called.
	assert.Equal(t, 0, transport.GetCallCountInfo()["GET /v2/storage/?exclude=components"])
}

func mockedClient() (client.Client, *httpmock.MockTransport) {
	c, transport := client.NewMockedClient()
	transport.RegisterResponder("GET", `/v2/storage/?exclude=components`, httpmock.NewJsonResponderOrPanic(200, &keboola.Index{
		Services: []*keboola.Service{
			{
				ID:  "queue",
				URL: "https://queue.keboola.mock",
			},
			{
				ID:  "scheduler",
				URL: "https://scheduler.keboola.mock",
			},
		},
		Features: keboola.Features{"dynamic-backend-size"},
	}))
	return c, transport
}
