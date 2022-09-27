package sandboxesapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndDeleteSandbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, sapiClient, queueClient, sandboxClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, sapiClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	var configId sandboxesapi.ConfigID
	var sandboxId sandboxesapi.SandboxID

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create sandbox
	{
		// Create python sandbox
		config, err := sandboxesapi.Create(
			ctx,
			sapiClient,
			queueClient,
			branch.ID,
			"test",
			sandboxesapi.TypePython,
			sandboxesapi.WithExpireAfterHours(1),
			sandboxesapi.WithSize(sandboxesapi.SizeMedium),
		)
		assert.NoError(t, err)
		assert.NotNil(t, config)

		id, err := sandboxesapi.GetSandboxID(config)
		assert.NoError(t, err)

		configId, sandboxId = config.ID, id

		// Get sandbox
		instance, err := sandboxesapi.GetRequest(sandboxId).Send(ctx, sandboxClient)
		assert.NoError(t, err)
		assert.NotNil(t, instance)
		assert.Equal(t, sandboxId, instance.ID)

		// List sandboxes
		instanceList, err := sandboxesapi.ListRequest().Send(ctx, sandboxClient)
		assert.NoError(t, err)
		foundInstance := false
		for _, v := range *instanceList {
			if v.ID == instance.ID {
				foundInstance = true
				break
			}
		}
		assert.True(t, foundInstance, "Sandbox instance list did not contain created instance")

		// List sandbox config
		configs, err := sandboxesapi.ListConfigRequest(branch.ID).Send(ctx, sapiClient)
		assert.NoError(t, err)
		assert.Len(t, *configs, 1)
		assert.Equal(t, config, (*configs)[0])
	}

	// Delete sandbox
	{
		err := sandboxesapi.Delete(
			ctx,
			sapiClient,
			queueClient,
			branch.ID,
			configId,
			sandboxId,
		)
		assert.NoError(t, err)
	}
}

func TestCreateAndDeleteSnowflakeSandbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, sapiClient, queueClient, sandboxClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, sapiClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create sandbox (both config and instance)
	config, err := sandboxesapi.Create(
		ctx,
		sapiClient,
		queueClient,
		branch.ID,
		"test-snowflake",
		sandboxesapi.TypeSnowflake,
		sandboxesapi.WithExpireAfterHours(1),
	)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	configId := config.ID
	sandboxId, err := sandboxesapi.GetSandboxID(config)
	assert.NoError(t, err)

	// Get sandbox
	instance, err := sandboxesapi.GetRequest(sandboxId).Send(ctx, sandboxClient)
	assert.NoError(t, err)
	assert.NotNil(t, instance)
	assert.Equal(t, sandboxId, instance.ID)

	// List sandboxes
	instanceList, err := sandboxesapi.ListRequest().Send(ctx, sandboxClient)
	assert.NoError(t, err)
	foundInstance := false
	for _, v := range *instanceList {
		if v.ID == instance.ID {
			foundInstance = true
			break
		}
	}
	assert.True(t, foundInstance, "Sandbox instance list did not contain created instance")

	// Delete sandbox (both config and instance)
	err = sandboxesapi.Delete(
		ctx,
		sapiClient,
		queueClient,
		branch.ID,
		configId,
		sandboxId,
	)
	assert.NoError(t, err)
}

func clientsForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender, client.Sender, client.Sender) {
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
	services := index.AllServices()
	jobsQueueHost, found := services.URLByID("queue")
	assert.True(t, found)
	sandboxHost, found := services.URLByID("sandboxes")
	assert.True(t, found)

	// Get Queue client
	jobsQueueApiClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	// Get Sandbox client
	sandboxApiClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxHost.String(), project.StorageAPIToken())

	return project, storageApiClient, jobsQueueApiClient, sandboxApiClient
}
