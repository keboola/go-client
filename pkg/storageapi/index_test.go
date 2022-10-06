package storageapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestIndexRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project := testproject.GetTestProject(t)
	c := ClientWithHost(client.NewTestClient(), project.StorageAPIHost())
	result, err := IndexRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Features)
	assert.NotEmpty(t, result.Services)
	_, found := result.AllServices().URLByID("encryption")
	assert.True(t, found)
	_, found = result.AllServices().URLByID("scheduler")
	assert.True(t, found)
}

func TestIndexRequest_WithoutToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project := testproject.GetTestProject(t)
	c := ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	result, err := IndexRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Features)
	assert.NotEmpty(t, result.Services)
	_, found := result.AllServices().URLByID("encryption")
	assert.True(t, found)
	_, found = result.AllServices().URLByID("scheduler")
	assert.True(t, found)
}

func TestIndexComponents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project := testproject.GetTestProject(t)
	c := ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	result, err := IndexComponentsRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Components)
	assert.NotEmpty(t, result.Features)
	assert.NotEmpty(t, result.Services)
	_, found := result.AllServices().URLByID("encryption")
	assert.True(t, found)
	_, found = result.AllServices().URLByID("scheduler")
	assert.True(t, found)
}

func TestIndexComponents_WithoutToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := ClientWithHost(client.NewTestClient(), `connection.keboola.com`)
	result, err := IndexComponentsRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Components)
	assert.NotEmpty(t, result.Features)
	assert.NotEmpty(t, result.Services)
	// Services
	url, found := result.AllServices().URLByID("encryption")
	assert.True(t, found)
	assert.Equal(t, ServiceURL("https://encryption.keboola.com"), url)
	url, found = result.AllServices().URLByID("scheduler")
	assert.True(t, found)
	assert.Equal(t, ServiceURL("https://scheduler.keboola.com"), url)
}
