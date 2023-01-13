package keboola_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
)

func TestSchedulerApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)
	c := client.NewTestClient()
	api := keboola.NewAPI(project.StorageAPIHost(), keboola.WithClient(&c), keboola.WithToken(project.StorageAPIToken()))

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// Create a config to schedule
	targetConfig := &keboola.ConfigWithRows{
		Config: &keboola.Config{
			ConfigKey: keboola.ConfigKey{
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
	_, err = api.CreateConfigRequest(targetConfig).Send(ctx)
	assert.NoError(t, err)

	// Create scheduler config
	schedulerConfig := &keboola.ConfigWithRows{
		Config: &keboola.Config{
			ConfigKey: keboola.ConfigKey{
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
	_, err = api.CreateConfigRequest(schedulerConfig).Send(ctx)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err := api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate
	schedule, err := api.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete
	_, err = api.DeleteScheduleRequest(schedule.ScheduleKey).Send(ctx)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err = api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate again
	schedule, err = api.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete for configuration
	_, err = api.DeleteSchedulesForConfigurationRequest(schedulerConfig.ID).Send(ctx)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err = api.ListSchedulesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)
}
