// Package jobsqueueapi contains request definitions for the Jobs Queue API.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host in the HTTP client, see the ClientWithHost function.
package jobsqueueapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/go-client/pkg/client"
)

// ComponentID is id of a Keboola component.
type ComponentID = keboola.ComponentID

// ConfigID is id of a Keboola component configuration.
type ConfigID = keboola.ConfigID

// ClientWithHostAndToken returns HTTP client with api host and token set.
func ClientWithHostAndToken(c client.Client, apiHost string, apiToken string) client.Client {
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return c.WithBaseURL(`https://`+apiHost).WithHeader("X-StorageApi-Token", apiToken)
}

func newRequest() client.HTTPRequest {
	// Create request and set default error type
	return client.NewHTTPRequest().WithError(&Error{})
}

// CreateJobRequest - https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/createJob
func CreateJobRequest(componentID ComponentID, configID ConfigID) client.APIRequest[*Job] {
	data := map[string]string{
		"component": componentID.String(),
		"mode":      "run",
		"config":    configID.String(),
	}
	result := Job{}
	request := newRequest().
		WithResult(&result).
		WithMethod(http.MethodPost).
		WithURL("jobs").
		WithJSONBody(data)
	return client.NewAPIRequest(&result, request)
}

// CreateJobConfigDataRequest - https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/createJob
//
// Allows setting configData.
//
// `configId` can be set to an empty string and it will be omitted.
func CreateJobConfigDataRequest(componentID ComponentID, configId ConfigID, configData map[string]any) client.APIRequest[*Job] {
	body := map[string]any{
		"component":  componentID.String(),
		"mode":       "run",
		"configData": configData,
	}
	if len(configId.String()) > 0 {
		body["config"] = configId.String()
	}

	result := &Job{}
	request := newRequest().
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("jobs").
		WithJSONBody(body)
	return client.NewAPIRequest(result, request)
}

// GetJobRequest https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/getJob
func GetJobRequest(key JobKey) client.APIRequest[*Job] {
	return getJobRequest(&Job{JobKey: key})
}

func getJobRequest(job *Job) client.APIRequest[*Job] {
	request := newRequest().
		WithResult(job).
		WithGet("jobs/{jobId}").
		AndPathParam("jobId", job.ID.String())
	return client.NewAPIRequest(job, request)
}

// WaitForJob pulls job status until it is completed.
func WaitForJob(ctx context.Context, sender client.Sender, job *Job) error {
	_, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("timeout for the job was not set")
	}

	retry := newJobBackoff()
	for {
		// Get job status
		if err := getJobRequest(job).SendOrErr(ctx, sender); err != nil {
			return err
		}

		// Check status
		if job.IsFinished {
			if job.Status == "success" {
				return nil
			}
			return fmt.Errorf(`job "%s" failed: %v`, job.ID, job.Result.Message)
		}

		// Wait and check again
		select {
		case <-ctx.Done():
			return fmt.Errorf(`error while waiting for the job "%s" to complete: %w`, job.ID, ctx.Err())
		case <-time.After(retry.NextBackOff()):
			// try again
		}
	}
}

// newBackoff creates retry for waitForJob.
func newJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 3 * time.Second
	b.Multiplier = 2
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0 // no limit, run until context timeout
	b.Reset()
	return b
}
