package keboola_test

import (
	"context"
	"net/http"
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
	api, err := keboola.NewAPI(ctx, "https://connection.keboola.mock", keboola.WithClient(&c))
	assert.NoError(t, err)
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

	api := keboola.NewAPIFromIndex("https://connection.keboola.mock", idx, keboola.WithClient(&c))
	assert.Equal(t, idx.Services, api.Index().Services)
	assert.Equal(t, idx.Features, api.Index().Features)

	// Index request was not called.
	assert.Equal(t, 0, transport.GetCallCountInfo()["GET /v2/storage/?exclude=components"])
}

func TestAPI_WithToken(t *testing.T) {
	t.Parallel()

	// Setup
	c, transport := mockedClient()
	ctx := context.Background()
	apiWithoutToken, err := keboola.NewAPI(ctx, "https://connection.keboola.mock", keboola.WithClient(&c))
	assert.NoError(t, err)

	// Register empty list buckets response
	transport.RegisterResponder(http.MethodGet, "/v2/storage/branch/123/buckets", func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, "my-token", request.Header.Get("X-StorageApi-Token"))
		return httpmock.NewStringResponse(http.StatusOK, "[]"), nil
	})

	// Test WithToken method
	apiWithToken := apiWithoutToken.WithToken("my-token")
	assert.NotSame(t, apiWithToken, apiWithoutToken) // value should be cloned
	assert.NoError(t, apiWithToken.ListBucketsRequest(123).SendOrErr(ctx))
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET /v2/storage/?exclude=components"])
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET /v2/storage/branch/123/buckets"])
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
