package sandbox_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/sandbox"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndDeleteSandbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, sapiClient, queueClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, sapiClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	var configId sandbox.ConfigID
	var sandboxId sandbox.SandboxID

	// Create sandbox
	{
		// Create sandbox config (so UI can see it)
		sandboxConfig, err := sandbox.CreateConfigRequest(branch.ID, "test").Send(ctx, sapiClient)
		assert.NoError(t, err)
		assert.NotNil(t, sandboxConfig)

		// Create sandbox from config
		params := sandbox.Params{
			Type:             "python",
			Shared:           false,
			ExpireAfterHours: 1,
			Size:             sandbox.SizeSmall,
		}
		_, err = sandbox.CreateJobRequest(sandboxConfig.ID, params).Send(ctx, queueClient)
		assert.NoError(t, err)

		// Get sandbox config
		// The initial config does not have the sandbox id, because the sandbox has not been created yet,
		// so we need to fetch the sandbox config after the sandbox create job finishes.
		// The sandbox id is separate from the sandbox config id, and we need both to delete the sandbox.
		config, err := sandbox.GetConfigRequest(branch.ID, sandboxConfig.ID).Send(ctx, sapiClient)
		assert.NoError(t, err)
		assert.NotNil(t, config)

		configId = config.ID
		idParam, found, err := config.Content.GetNested("parameters.id")
		assert.NoError(t, err)
		assert.True(t, found, "configuration is missing parameters.id")
		sandboxId = sandbox.SandboxID(idParam.(string))
	}

	// Delete sandbox
	{
		// Delete sandbox (this stops the instance and deletes it)
		_, err := sandbox.DeleteJobRequest(configId, sandboxId).Send(ctx, queueClient)
		assert.NoError(t, err)

		// Delete sandbox config (so it is no longer visible in UI)
		_, err = sandbox.DeleteConfigRequest(branch.ID, configId).Send(ctx, sapiClient)
		assert.NoError(t, err)
	}
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

	// Get Queue API and Sandboxes API hosts
	index, err := storageapi.IndexRequest().Send(ctx, storageApiClient)
	assert.NoError(t, err)
	jobsQueueHost, found := index.AllServices().URLByID("queue")
	assert.True(t, found)

	// Get Queue client
	jobsQueueApiClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	return project, storageApiClient, jobsQueueApiClient
}
