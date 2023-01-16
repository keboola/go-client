package keboola_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
)

func TestQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	api := keboola.APIClientForAnEmptyProject(t)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config
	config := &keboola.ConfigWithRows{
		Config: &keboola.Config{
			ConfigKey: keboola.ConfigKey{
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
		Rows: []*keboola.ConfigRow{},
	}
	resConfig, err := api.CreateConfigRequest(config).Send(ctx)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	resJob, err := api.CreateQueueJobRequest("ex-generic-v2", config.ID).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, resJob.ID)

	// Wait for the job
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()
	err = api.WaitForQueueJob(timeoutCtx, resJob)
	// The job payload is malformed, so it fails. We are checking just that it finished.
	assert.ErrorContains(t, err, "Unrecognized option \"foo\" under \"container\"")
}

func TestQueueWaitForQueueJobTimeout(t *testing.T) {
	t.Parallel()

	job := keboola.QueueJob{
		JobKey:     keboola.JobKey{ID: "1234"},
		Status:     "waiting",
		IsFinished: false,
	}

	// Trace client activity
	var trace bytes.Buffer

	// Create mocked timeout
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://connection.test").AndTrace(client.LogTracer(&trace))
	transport.RegisterResponder("GET", `https://connection.test/v2/storage/?exclude=components`, newJSONResponder(200, `{
		"services": [
			{
				"id": "queue",
				"url": "https://queue.connection.test"
			}
		],
		"features": []
	}`))
	transport.RegisterResponder("GET", `=~^https://queue.connection.test/jobs/1234`, httpmock.NewJsonResponderOrPanic(200, job))
	api := keboola.NewAPI("https://connection.test", keboola.WithClient(&c))

	// Create context with deadline
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()

	// Error - deadline exceeded
	err := api.WaitForQueueJob(ctx, &job)
	assert.Error(t, err)
	assert.Equal(t, `error while waiting for the job "1234" to complete: context deadline exceeded`, err.Error())

	// Check calls count
	assert.Equal(t, 3, transport.GetCallCountInfo()["GET https://queue.connection.test/jobs/1234"])

	// Check client activity
	wildcards.Assert(t, strings.TrimSpace(`
HTTP_REQUEST[0001] START GET "https://connection.test/v2/storage/?exclude=components"
HTTP_REQUEST[0001] DONE  GET "https://connection.test/v2/storage/?exclude=components" | 200 | %s
HTTP_REQUEST[0001] BODY  GET "https://connection.test/v2/storage/?exclude=components" | %s
HTTP_REQUEST[0002] START GET "https://queue.connection.test/jobs/1234"
HTTP_REQUEST[0002] DONE  GET "https://queue.connection.test/jobs/1234" | 200 | %s
HTTP_REQUEST[0002] BODY  GET "https://queue.connection.test/jobs/1234" | %s
HTTP_REQUEST[0003] START GET "https://queue.connection.test/jobs/1234"
HTTP_REQUEST[0003] DONE  GET "https://queue.connection.test/jobs/1234" | 200 | %s
HTTP_REQUEST[0003] BODY  GET "https://queue.connection.test/jobs/1234" | %s
HTTP_REQUEST[0004] START GET "https://queue.connection.test/jobs/1234"
HTTP_REQUEST[0004] DONE  GET "https://queue.connection.test/jobs/1234" | 200 | %s
HTTP_REQUEST[0004] BODY  GET "https://queue.connection.test/jobs/1234" | %s
`), trace.String())
}
