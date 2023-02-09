package keboola

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCreateOldQueueJobRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := APIClientForAnEmptyProject(t, ctx, testproject.WithQueueV1())

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	config, err := api.CreateConfigRequest(&ConfigWithRows{
		Config: &Config{
			ConfigKey: ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "keboola-test.ex-connection-revision",
			},
			Name: "Create old queue job test config",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key:   "host",
							Value: "https://connection.keboola.com",
						},
					}),
				},
			}),
		},
	}).Send(ctx)
	assert.NoError(t, err)

	job, err := api.CreateOldQueueJobRequest(config.ComponentID, config.ID).Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	detail, err := api.GetOldQueueJobRequest(job.ID, WithMetrics()).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, detail.ID)
	assert.NotNil(t, detail.Metrics)
}

func TestCreateOldQueueJobRequestURL(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		"docker/my-component/run",
		initOldQueueJobConfig(
			ComponentID("my-component"),
			ConfigID("my-config"),
		).getURL(),
	)

	assert.Equal(t,
		"docker/my-component/run/tag/my-tag",
		initOldQueueJobConfig(
			ComponentID("my-component"),
			ConfigID("my-config"),
			WithImageTag("my-tag"),
		).getURL(),
	)

	assert.Equal(t,
		"docker/branch/1000/my-component/run",
		initOldQueueJobConfig(
			ComponentID("my-component"),
			ConfigID("my-config"),
			WithBranchID(BranchID(1000)),
		).getURL(),
	)

	assert.Equal(t,
		"docker/branch/1000/my-component/run/tag/my-tag",
		initOldQueueJobConfig(
			ComponentID("my-component"),
			ConfigID("my-config"),
			WithBranchID(BranchID(1000)),
			WithImageTag("my-tag"),
		).getURL(),
	)
}
