package keboola

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/client/trace"
)

func TestQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config
	config := &ConfigWithRows{
		Config: &Config{
			ConfigKey: ConfigKey{
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
		Rows: []*ConfigRow{},
	}
	resConfig, err := api.CreateConfigRequest(config).Send(ctx)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	resJob, err := api.NewCreateJobRequest("ex-generic-v2").WithConfig(config.ID).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, resJob.ID)

	// Wait for the job
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()
	err = api.WaitForQueueJob(timeoutCtx, resJob.ID)
	// The job payload is malformed, so it fails. We are checking just that it finished.
	assert.ErrorContains(t, err, "Unrecognized option \"foo\" under \"container\"")
}

func TestCreateQueueJobRequestBuilder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	builder := api.NewCreateJobRequest("ex-generic-v2").
		WithTag("latest").
		WithBranch(1234).
		WithConfig("987654321").
		WithConfigRowIDs([]string{"config-row-a", "config-row-b"}).
		WithConfigData(map[string]any{"a": "b"}).
		WithBackendSize("xsmall").
		WithVariableValuesID("variable-values-id").
		WithVariableValuesData([]VariableData{{Name: "a", Value: "b"}})

	data, err := json.MarshalIndent(builder.config, "", "  ")
	assert.NoError(t, err)
	assert.Equal(t,
		`{
  "tag": "latest",
  "branchId": 1234,
  "component": "ex-generic-v2",
  "config": "987654321",
  "configRowIds": [
    "config-row-a",
    "config-row-b"
  ],
  "configData": {
    "a": "b"
  },
  "variableValuesId": "variable-values-id",
  "variableValuesData": {
    "values": [
      {
        "name": "a",
        "value": "b"
      }
    ]
  },
  "backend": "xsmall"
}`,
		string(data),
	)
}

func TestQueueWaitForQueueJobTimeout(t *testing.T) {
	t.Parallel()

	job := QueueJob{
		JobKey:     JobKey{ID: "1234"},
		Status:     "waiting",
		IsFinished: false,
	}

	// Trace client activity
	var traceOut bytes.Buffer

	// Create mocked timeout
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://connection.test").AndTrace(trace.LogTracer(&traceOut))
	transport.RegisterResponder("GET", `https://connection.test/v2/storage/?exclude=components`, newJSONResponder(`{
		"services": [
			{
				"id": "queue",
				"url": "https://queue.connection.test"
			}
		],
		"features": []
	}`))
	transport.RegisterResponder("GET", `=~^https://queue.connection.test/jobs/1234`, httpmock.NewJsonResponderOrPanic(200, job))
	api, err := NewAuthorizedAPI(context.Background(), "https://connection.test", "my-token", WithClient(&c))
	assert.NoError(t, err)

	// Create context with deadline
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()

	// Error - deadline exceeded
	err = api.WaitForQueueJob(ctx, job.ID)
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
`), traceOut.String())
}

func TestDeprecatedQueueApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config
	config := &ConfigWithRows{
		Config: &Config{
			ConfigKey: ConfigKey{
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
		Rows: []*ConfigRow{},
	}
	resConfig, err := api.CreateConfigRequest(config).Send(ctx)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)

	// Run a job on the config
	job, err := api.NewCreateJobRequest("ex-generic-v2").WithConfig(config.ID).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, job.ID)

	// Wait for the job
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFn()
	err = api.WaitForQueueJob(timeoutCtx, job.ID)
	// The job payload is malformed, so it fails. We are checking just that it finished.
	assert.ErrorContains(t, err, "Unrecognized option \"foo\" under \"container\"")
}

func newJSONResponder(response string) httpmock.Responder {
	r := httpmock.NewStringResponse(200, response)
	r.Header.Set("Content-Type", "application/json")
	return httpmock.ResponderFromResponse(r)
}
