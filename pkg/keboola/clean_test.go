package keboola_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
)

func TestCleanProject(t *testing.T) {
	t.Parallel()

	ctx, project, api := deps(t)

	// Clean project
	if err := keboola.CleanProject(ctx, api); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Assert that project is clean

	// Only default branch exists
	branches, err := api.ListBranchesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *branches, 1)
	defaultBranch := (*branches)[0].BranchKey

	// Default branch has no metadata
	metadata, err := api.ListBranchMetadataRequest(defaultBranch).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *metadata, 0)

	// No configs - implies no rows or config metadata
	configs, err := api.ListConfigsAndRowsFrom(defaultBranch).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *configs, 0)

	// No buckets - implies no tables
	buckets, err := api.ListBucketsRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *buckets, 0)

	// No schedules
	schedules, err := api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// No sandbox instances
	instances, err := api.ListWorkspaceInstancesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *instances, 0)
}

func deps(t *testing.T) (context.Context, *testproject.Project, *keboola.API) {
	t.Helper()

	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)

	c := client.NewTestClient()
	api := keboola.NewAPI(project.StorageAPIHost(), keboola.WithClient(&c), keboola.WithToken(project.StorageAPIToken()))

	return ctx, project, api
}
