package jobsqueueapi_test

import (
	"context"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJobsQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, storageClient, jobsQueueClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := storageapi.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config
	config := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "foo", Value: "bar"},
			}),
		},
		Rows: []*storageapi.ConfigRow{},
	}
	resConfig, err := storageapi.CreateConfigRequest(config).Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	resJob, err := jobsqueueapi.CreateJobRequest("ex-generic-v2", config.ID).Send(ctx, jobsQueueClient)
	assert.NoError(t, err)
	assert.NotEmpty(t, resJob.ID)

	// Wait for the job
	err = jobsqueueapi.WaitForJob(ctx, jobsQueueClient, resJob)
	// The job payload is malformed, so it fails. We are checking just that it finished.
	assert.ErrorContains(t, err, "Unrecognized option \"foo\" under \"container\"")
}

func clientsForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender, client.Sender) {
	ctx := context.Background()
	project := testproject.GetTestProject(t)

	// Get Storage API client
	storageApiClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	// Clean project
	if _, err := storageapi.CleanProjectRequest().Send(ctx, storageApiClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Get Queue API host
	index, err := storageapi.IndexRequest().Send(ctx, storageApiClient)
	assert.NoError(t, err)
	jobsQueueHost, found := index.AllServices().URLByID("queue")
	assert.True(t, found)

	// Get Queue client
	jobsQueueApiClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	return project, storageApiClient, jobsQueueApiClient
}
