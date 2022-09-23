package sandbox_test

import (
	"context"
	"testing"
	"time"

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

	var (
		configId  sandbox.ConfigID
		sandboxId sandbox.SandboxID
	)

	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()

	// Create sandbox
	{
		s, err := sandbox.Create(
			timeoutCtx,
			sapiClient,
			queueClient,
			branch.ID,
			"test",
			"python",
			sandbox.WithExpireAfterHours(1),
			sandbox.WithSize(sandbox.SizeMedium),
		)
		assert.NoError(t, err)
		assert.NotNil(t, s)

		id, err := sandbox.GetSandboxID(s)
		assert.NoError(t, err)

		configId, sandboxId = s.ID, id

		// List sandbox config
		configs, err := sandbox.ListConfigRequest(branch.ID).Send(ctx, sapiClient)
		assert.NoError(t, err)
		assert.Len(t, *configs, 1)
		assert.Equal(t, s, (*configs)[0])
	}

	// Delete sandbox
	{
		err := sandbox.Delete(
			timeoutCtx,
			sapiClient,
			queueClient,
			branch.ID,
			configId,
			sandboxId,
		)
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
