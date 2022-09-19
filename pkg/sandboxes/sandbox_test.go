package sandboxes_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxes"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndDeleteSandbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	var configId storageapi.ConfigID
	// Create sandbox
	{
		params := sandboxes.SandboxParams{
			Name:             "test",
			Type:             "python",
			Shared:           false,
			ExpireAfterHours: 1,
			Size:             sandboxes.SandboxSizeSmall,
		}
		config, err := sandboxes.CreateSandboxRequest(branch.ID, params).Send(ctx, c)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		configId = config.ID
	}
	// Delete sandbox
	{
		_, err := sandboxes.DeleteSandboxRequest(configId).Send(ctx, c)
		assert.NoError(t, err)
	}
}

func clientForRandomProject(t *testing.T) (*testproject.Project, client.Client) {
	project := testproject.GetTestProject(t)
	c := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	return project, c
}

func clientForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender) {
	project, c := clientForRandomProject(t)
	_, err := storageapi.CleanProjectRequest().Send(context.Background(), c)
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return project, c
}
