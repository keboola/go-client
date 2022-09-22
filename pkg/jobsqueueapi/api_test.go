package jobsqueueapi_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
)

func TestJobsQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, storageClient, jobsQueueClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := storageapi.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, storageClient)
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
	resConfig, err := storageapi.CreateConfigRequest(config).Send(ctx, storageClient)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	resJob, err := jobsqueueapi.CreateJobRequest("ex-generic-v2", config.ID).Send(ctx, jobsQueueClient)
	assert.NoError(t, err)
	assert.NotEmpty(t, resJob.ID)

	// Wait for the job
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()
	err = jobsqueueapi.WaitForJob(timeoutCtx, jobsQueueClient, resJob)
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
	assert.Equal(t, `timeout while waiting for the component job "1234" to complete: context deadline exceeded`, err.Error())

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

func clientsForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender, client.Sender) {
	ctx := context.Background()
	project := testproject.GetTestProject(t)

	// Get Storage API client
	storageApiClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	// Clean project
	if _, err := storageapi.CleanProjectRequest().Send(ctx, storageApiClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Get Queue API host
	index, err := storageapi.IndexRequest().Send(ctx, storageApiClient)
	assert.NoError(t, err)
	jobsQueueHost, found := index.AllServices().URLByID("queue")
	assert.True(t, found)

	// Get Queue client
	jobsQueueApiClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	return project, storageApiClient, jobsQueueApiClient
}
