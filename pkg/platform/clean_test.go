package platform_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
)

func TestCleanProject(t *testing.T) {
	t.Parallel()

	ctx, project, c := deps(t)

	// Clean project
	if err := platform.CleanProject(ctx, c.Storage, c.Schedule, c.Queue, c.Sandbox); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Assert that project is clean

	// Only default branch exists
	branches, err := keboola.ListBranchesRequest().Send(ctx, c.Storage)
	assert.NoError(t, err)
	assert.Len(t, *branches, 1)
	defaultBranch := (*branches)[0].BranchKey

	// Default branch has no metadata
	metadata, err := keboola.ListBranchMetadataRequest(defaultBranch).Send(ctx, c.Storage)
	assert.NoError(t, err)
	assert.Len(t, *metadata, 0)

	// No configs - implies no rows or config metadata
	configs, err := keboola.ListConfigsAndRowsFrom(defaultBranch).Send(ctx, c.Storage)
	assert.NoError(t, err)
	assert.Len(t, *configs, 0)

	// No buckets - implies no tables
	buckets, err := keboola.ListBucketsRequest().Send(ctx, c.Storage)
	assert.NoError(t, err)
	assert.Len(t, *buckets, 0)

	// No schedules
	schedules, err := keboola.ListSchedulesRequest().Send(ctx, c.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// No sandbox instances
	instances, err := sandboxesapi.ListInstancesRequest().Send(ctx, c.Sandbox)
	assert.NoError(t, err)
	assert.Len(t, *instances, 0)
}

type testClients struct {
	Storage  client.Sender
	Schedule client.Sender
	Sandbox  client.Sender
	Queue    client.Sender
}

func deps(t *testing.T) (context.Context, *testproject.Project, *testClients) {
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

	scheduleClient := keboola.ClientWithHostAndToken(client.NewTestClient(), schedulerApiHost.String(), project.StorageAPIToken())
	sandboxClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxesApiHost.String(), project.StorageAPIToken())
	queueClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	clients := &testClients{
		Storage:  api,
		Schedule: scheduleClient,
		Sandbox:  sandboxClient,
		Queue:    queueClient,
	}

	return ctx, project, clients
}
