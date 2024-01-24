package keboola_test

import (
	"context"
	"github.com/keboola/go-utils/pkg/testproject"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
)

func TestWorkspacesCreateAndDeletePython(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := keboola.APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create workspace
	workspace, err := api.CreateWorkspace(
		ctx,
		branch.ID,
		"test",
		keboola.WorkspaceTypePython,
		keboola.WithExpireAfterHours(1),
		keboola.WithSize(keboola.WorkspaceSizeMedium),
	)
	assert.NoError(t, err)
	assert.NotNil(t, workspace)

	// List workspaces - try to find the one we just created
	workspaces, err := api.ListWorkspaces(ctx, branch.ID)
	assert.NoError(t, err)
	foundInstance := false
	for _, v := range workspaces {
		if workspace.Workspace.ID == v.Workspace.ID {
			foundInstance = true
			break
		}
	}
	assert.True(t, foundInstance, "Workspace list did not find created workspace")

	// Delete workspace
	err = api.DeleteWorkspace(
		ctx,
		branch.ID,
		workspace.Config.ID,
		workspace.Workspace.ID,
	)
	assert.NoError(t, err)
}

func TestWorkspacesCreateAndDeleteSnowflake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := keboola.APIClientForAnEmptyProject(t, ctx, testproject.WithSnowflakeBackend())

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	ctx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()

	// Create workspace
	workspace, err := api.CreateWorkspace(
		ctx,
		branch.ID,
		"test-snowflake",
		keboola.WorkspaceTypeSnowflake,
		keboola.WithExpireAfterHours(1),
	)
	assert.NoError(t, err)
	assert.NotNil(t, workspace)

	// List workspaces - try to find the one we just created
	workspaces, err := api.ListWorkspaces(ctx, branch.ID)
	assert.NoError(t, err)
	foundInstance := false
	for _, v := range workspaces {
		if workspace.Workspace.ID == v.Workspace.ID {
			foundInstance = true
			break
		}
	}
	assert.True(t, foundInstance, "Workspace list did not find created workspace")

	// Delete workspace
	err = api.DeleteWorkspace(
		ctx,
		branch.ID,
		workspace.Config.ID,
		workspace.Workspace.ID,
	)
	assert.NoError(t, err)
}
