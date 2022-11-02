package jobsqueueapi_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/platform"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
)

func TestJobsQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx, c := depsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := storageapi.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config
	config := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "foo", Value: "bar"},
			}),
		},
		Rows: []*storageapi.ConfigRow{},
	}
	resConfig, err := storageapi.CreateConfigRequest(config).Send(ctx, c.StorageClient)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	resJob, err := jobsqueueapi.CreateJobRequest("ex-generic-v2", config.ID).Send(ctx, c.QueueClient)
	assert.NoError(t, err)
	assert.NotEmpty(t, resJob.ID)

	// Wait for the job
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()
	err = jobsqueueapi.WaitForJob(timeoutCtx, c.QueueClient, resJob)
	// The job payload is malformed, so it fails. We are checking just that it finished.
	assert.ErrorContains(t, err, "Unrecognized option \"foo\" under \"container\"")
}

func TestWaitForJobTimeout(t *testing.T) {
	t.Parallel()

	job := jobsqueueapi.Job{
		JobKey:     jobsqueueapi.JobKey{ID: "1234"},
		Status:     "waiting",
		IsFinished: false,
	}

	// Trace client activity
	var trace bytes.Buffer

	// Create mocked timeout
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com").AndTrace(client.LogTracer(&trace))
	transport.RegisterResponder("GET", `=~^https://example.com/jobs/1234`, httpmock.NewJsonResponderOrPanic(200, job))

	// Create context with deadline
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()

	// Error - deadline exceeded
	err := jobsqueueapi.WaitForJob(ctx, c, &job)
	assert.Error(t, err)
	assert.Equal(t, `error while waiting for the job "1234" to complete: context deadline exceeded`, err.Error())

	// Check calls count
	assert.Equal(t, 3, transport.GetCallCountInfo()["GET https://example.com/jobs/1234"])

	// Check client activity
	wildcards.Assert(t, strings.TrimSpace(`
HTTP_REQUEST[0001] START GET "https://example.com/jobs/1234"
HTTP_REQUEST[0001] DONE  GET "https://example.com/jobs/1234" | 200 | %s
HTTP_REQUEST[0001] BODY  GET "https://example.com/jobs/1234" | %s
HTTP_REQUEST[0002] START GET "https://example.com/jobs/1234"
HTTP_REQUEST[0002] DONE  GET "https://example.com/jobs/1234" | 200 | %s
HTTP_REQUEST[0002] BODY  GET "https://example.com/jobs/1234" | %s
HTTP_REQUEST[0003] START GET "https://example.com/jobs/1234"
HTTP_REQUEST[0003] DONE  GET "https://example.com/jobs/1234" | 200 | %s
HTTP_REQUEST[0003] BODY  GET "https://example.com/jobs/1234" | %s
`), trace.String())
}

type testClients struct {
	StorageClient   client.Sender
	SchedulerClient client.Sender
	SandboxClient   client.Sender
	QueueClient     client.Sender
}

func depsForAnEmptyProject(t *testing.T) (context.Context, *testClients) {
	t.Helper()

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	t.Cleanup(func() {
		cancel()
	})

	project, _ := testproject.GetTestProjectForTest(t)
	storageClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	index, err := storageapi.IndexRequest().Send(ctx, storageClient)
	assert.NoError(t, err)

	services := index.AllServices()
	schedulerApiHost, found := services.URLByID("scheduler")
	assert.True(t, found)
	sandboxesApiHost, found := services.URLByID("sandboxes")
	assert.True(t, found)
	jobsQueueHost, found := services.URLByID("queue")
	assert.True(t, found)

	schedulerClient := schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerApiHost.String(), project.StorageAPIToken())
	sandboxClient := sandboxesapi.ClientWithHostAndToken(client.NewTestClient(), sandboxesApiHost.String(), project.StorageAPIToken())
	queueClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	if err := platform.CleanProject(ctx, storageClient, schedulerClient, queueClient, sandboxClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	clients := &testClients{
		StorageClient:   storageClient,
		SchedulerClient: schedulerClient,
		SandboxClient:   sandboxClient,
		QueueClient:     queueClient,
	}

	return ctx, clients
}
