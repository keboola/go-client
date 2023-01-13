package sandboxesapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
)

func TestCreateAndDeletePythonSandbox(t *testing.T) {
	t.Parallel()
	ctx, clients := depsForAnEmptyProject(t)

	// Get default branch
	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, clients.Storage)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create sandbox
	sandbox, err := sandboxesapi.Create(
		ctx,
		clients.Storage,
		clients.Queue,
		clients.Sandbox,
		branch.ID,
		"test",
		sandboxesapi.TypePython,
		sandboxesapi.WithExpireAfterHours(1),
		sandboxesapi.WithSize(sandboxesapi.SizeMedium),
	)
	assert.NoError(t, err)
	assert.NotNil(t, sandbox)

	// List sandboxes - try to find the one we just created
	sandboxes, err := sandboxesapi.List(ctx, clients.Storage, clients.Sandbox, branch.ID)
	assert.NoError(t, err)
	foundInstance := false
	for _, v := range sandboxes {
		if sandbox.Sandbox.ID == v.Sandbox.ID {
			foundInstance = true
			break
		}
	}
	assert.True(t, foundInstance, "Sandbox list did not find created sandbox")

	// Delete sandbox
	err = sandboxesapi.Delete(
		ctx,
		clients.Storage,
		clients.Queue,
		branch.ID,
		sandbox.Config.ID,
		sandbox.Sandbox.ID,
	)
	assert.NoError(t, err)
}

func TestCreateAndDeleteSnowflakeSandbox(t *testing.T) {
	t.Parallel()
	ctx, clients := depsForAnEmptyProject(t)

	// Get default branch
	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, clients.Storage)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create sandbox
	sandbox, err := sandboxesapi.Create(
		ctx,
		clients.Storage,
		clients.Queue,
		clients.Sandbox,
		branch.ID,
		"test-snowflake",
		sandboxesapi.TypeSnowflake,
		sandboxesapi.WithExpireAfterHours(1),
	)
	assert.NoError(t, err)
	assert.NotNil(t, sandbox)

	// List sandboxes - try to find the one we just created
	sandboxes, err := sandboxesapi.List(ctx, clients.Storage, clients.Sandbox, branch.ID)
	assert.NoError(t, err)
	foundInstance := false
	for _, v := range sandboxes {
		if sandbox.Sandbox.ID == v.Sandbox.ID {
			foundInstance = true
			break
		}
	}
	assert.True(t, foundInstance, "Sandbox list did not find created sandbox")

	// Delete sandbox
	err = sandboxesapi.Delete(
		ctx,
		clients.Storage,
		clients.Queue,
		branch.ID,
		sandbox.Config.ID,
		sandbox.Sandbox.ID,
	)
	assert.NoError(t, err)
}

type testClients struct {
	Storage  client.Sender
	Schedule client.Sender
	Sandbox  client.Sender
	Queue    client.Sender
}

func depsForAnEmptyProject(t *testing.T) (context.Context, *testClients) {
	t.Helper()

	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)

	c := client.NewTestClient()
	api := keboola.NewAPI(project.StorageAPIHost(), keboola.WithClient(&c), keboola.WithToken(project.StorageAPIToken()))

	index, err := api.IndexRequest().Send(ctx)
	assert.NoError(t, err)

	services := index.AllServices()
	schedulerApiHost, found := services.URLByID("scheduler")
	assert.True(t, found)
	sandboxesApiHost, found := services.URLByID("sandboxes")
	assert.True(t, found)
	jobsQueueHost, found := services.URLByID("queue")
	assert.True(t, found)

	schedulerClient := keboola.ClientWithHostAndToken(client.NewTestClient(), schedulerApiHost.String(), project.StorageAPIToken())
	sandboxesClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxesApiHost.String(), project.StorageAPIToken())
	queueClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	if err := platform.CleanProject(ctx, api, schedulerClient, queueClient, sandboxesClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	clients := &testClients{
		Storage:  api,
		Schedule: schedulerClient,
		Sandbox:  sandboxesClient,
		Queue:    queueClient,
	}

	return ctx, clients
}
