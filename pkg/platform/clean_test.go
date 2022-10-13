package platform_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
)

func TestCleanProject(t *testing.T) {
	t.Parallel()

	ctx, project, c := deps(t)

	// Clean project
	if err := platform.CleanProject(ctx, c.StorageClient, c.ScheduleClient, c.SandboxClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Assert that project is clean

	// Only default branch exists
	branches, err := storageapi.ListBranchesRequest().Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Len(t, *branches, 1)
	defaultBranch := (*branches)[0].BranchKey

	// Default branch has no metadata
	metadata, err := storageapi.ListBranchMetadataRequest(defaultBranch).Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Len(t, *metadata, 0)

	// No configs - implies no rows or config metadata
	configs, err := storageapi.ListConfigsAndRowsFrom(defaultBranch).Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Len(t, *configs, 0)

	// No buckets - implies no tables
	buckets, err := storageapi.ListBucketsRequest().Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Len(t, *buckets, 0)

	// No schedules
	schedules, err := schedulerapi.ListSchedulesRequest().Send(ctx, c.ScheduleClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// No sandbox instances
	instances, err := sandboxesapi.ListInstancesRequest().Send(ctx, c.SandboxClient)
	assert.NoError(t, err)
	assert.Len(t, *instances, 0)
}

type testClients struct {
	StorageClient  client.Sender
	ScheduleClient client.Sender
	SandboxClient  client.Sender
	QueueClient    client.Sender
}

func deps(t *testing.T) (context.Context, *testproject.Project, *testClients) {
	t.Helper()

	ctx := context.Background()
	project := testproject.GetTestProject(t)

	storageClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	index, err := storageapi.IndexRequest().Send(ctx, storageClient)
	assert.NoError(t, err)

	services := index.AllServices()
	schedulerApiHost, found := services.URLByID("scheduler")
	assert.True(t, found)
	sandboxesApiHost, found := services.URLByID("sandboxes")
	assert.True(t, found)
	jobsQueueHost, found := services.URLByID("queue")
	assert.True(t, found)

	scheduleClient := schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerApiHost.String(), project.StorageAPIToken())
	sandboxClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxesApiHost.String(), project.StorageAPIToken())
	queueClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	clients := &testClients{
		StorageClient:  storageClient,
		ScheduleClient: scheduleClient,
		SandboxClient:  sandboxClient,
		QueueClient:    queueClient,
	}

	return ctx, project, clients
}
