package schedulerapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
)

func TestSchedulerApiCalls(t *testing.T) {
	t.Parallel()
	ctx, clients := depsForAnEmptyProject(t)

	// Get default branch
	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, clients.Storage)
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
	_, err = keboola.CreateConfigRequest(targetConfig).Send(ctx, clients.Storage)
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
	_, err = keboola.CreateConfigRequest(schedulerConfig).Send(ctx, clients.Storage)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err := schedulerapi.ListSchedulesRequest().Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate
	schedule, err := schedulerapi.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete
	_, err = schedulerapi.DeleteScheduleRequest(schedule.ScheduleKey).Send(ctx, clients.Schedule)
	assert.NoError(t, err)

	// List should return no scheduleW
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)

	// Activate again
	schedule, err = schedulerapi.ActivateScheduleRequest(schedulerConfig.ID, "").Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.ID)

	// List should return one schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 1)

	// Delete for configuration
	_, err = schedulerapi.DeleteSchedulesForConfigurationRequest(schedulerConfig.ID).Send(ctx, clients.Schedule)
	assert.NoError(t, err)

	// List should return no schedule
	schedules, err = schedulerapi.ListSchedulesRequest().Send(ctx, clients.Schedule)
	assert.NoError(t, err)
	assert.Len(t, *schedules, 0)
}

type testClients struct {
	Storage  client.Sender
	Schedule client.Sender
	Sandbox  client.Sender
	Queue    client.Sender
}

func depsForAnEmptyProject(t *testing.T) (context.Context, *testClients) {
	t.Helper()

	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)

	storageClient := keboola.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	index, err := keboola.IndexRequest().Send(ctx, storageClient)
	assert.NoError(t, err)

	services := index.AllServices()
	schedulerApiHost, found := services.URLByID("scheduler")
	assert.True(t, found)
	sandboxesApiHost, found := services.URLByID("sandboxes")
	assert.True(t, found)
	jobsQueueHost, found := services.URLByID("queue")
	assert.True(t, found)

	scheduleClient := schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerApiHost.String(), project.StorageAPIToken())
	sandboxClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxesApiHost.String(), project.StorageAPIToken())
	queueClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	if err := platform.CleanProject(ctx, storageClient, scheduleClient, queueClient, sandboxClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	clients := &testClients{
		Storage:  storageClient,
		Schedule: scheduleClient,
		Sandbox:  sandboxClient,
		Queue:    queueClient,
	}

	return ctx, clients
}
