package platform_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCleanProject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	project := testproject.GetTestProject(t)

	// Get Storage API client
	storageClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	// Get API hosts
	index, err := storageapi.IndexRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	services := index.AllServices()

	// Get Sandbox client
	sandboxHost, found := services.URLByID("sandboxes")
	assert.True(t, found)
	sandboxClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxHost.String(), project.StorageAPIToken())

	// Get Scheduler client
	schedulerHost, found := services.URLByID("scheduler")
	assert.True(t, found)
	schedulerClient := schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerHost.String(), project.StorageAPIToken())

	// Clean project
	if err := platform.CleanProject(ctx, storageClient, schedulerClient, sandboxClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Assert that project is clean

	// Only default branch exists
	branches, err := storageapi.ListBranchesRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Len(t, *branches, 1)
	defaultBranch := (*branches)[0].BranchKey

	// Default branch has no metadata
	metadata, err := storageapi.ListBranchMetadataRequest(defaultBranch).Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Len(t, *metadata, 0)

	// No configs - implies no rows or config metadata
	configs, err := storageapi.ListConfigsAndRowsFrom(defaultBranch).Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Len(t, *configs, 0)

	// No buckets - implies no tables
	buckets, err := storageapi.ListBucketsRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Len(t, *buckets, 0)

	// No schedules
	schedules, err := schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// No sandbox instances
	instances, err := sandboxesapi.ListInstancesRequest().Send(ctx, sandboxClient)
	assert.NoError(t, err)
	assert.Len(t, *instances, 0)
}
