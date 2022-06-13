package schedulerapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestSchedulerApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project := testproject.GetTestProject(t)

	// Get storage client and clear project
	storageClient := storageapi.APIClientWithToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	_, err := storageapi.CleanProjectRequest().Send(ctx, storageClient)
	assert.NoError(t, err)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// Create a config to schedule
	targetConfig := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
		},
	}
	_, err = storageapi.CreateConfigRequest(targetConfig).Send(ctx, storageClient)
	assert.NoError(t, err)

	// Create scheduler config
	schedulerConfig := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "keboola.scheduler",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "schedule",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "cronTab", Value: "*/2 * * * *"},
						{Key: "timezone", Value: "UTC"},
						{Key: "state", Value: "disabled"},
					}),
				},
				{
					Key: "target",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "componentId", Value: "ex-generic-v2"},
						{Key: "configurationId", Value: targetConfig.ID},
						{Key: "mode", Value: "run"},
					}),
				},
			}),
		},
	}
	_, err = storageapi.CreateConfigRequest(schedulerConfig).Send(ctx, storageClient)
	assert.NoError(t, err)

	// Get Scheduler API host
	index, err := storageapi.IndexRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	schedulerHost, found := index.AllServices().URLByID("scheduler")
	assert.True(t, found)

	// Get scheduler client
	schedulerClient := schedulerapi.APIClient(client.NewTestClient(), schedulerHost.String(), project.StorageAPIToken())

	// List should return no schedule
	schedules, err := schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate
	schedule, err := schedulerapi.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete
	_, err = schedulerapi.DeleteScheduleRequest(schedule.ScheduleKey).Send(ctx, schedulerClient)
	assert.NoError(t, err)

	// List should return no scheduleW
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate again
	schedule, err = schedulerapi.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete for configuration
	_, err = schedulerapi.DeleteSchedulesForConfigurationRequest(schedulerConfig.ID).Send(ctx, schedulerClient)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, schedulerClient)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)
}
