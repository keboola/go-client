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
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
)

// ComponentID is id of a Keboola component.
type ComponentID = storageapi.ComponentID

// ConfigID is id of a Keboola component configuration.
type ConfigID = storageapi.ConfigID

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

// https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/createJob
//
// Allows setting `configData`
func CreateJobConfigDataRequest(componentID ComponentID, configID ConfigID, configData map[string]any) client.APIRequest[*Job] {
	data := map[string]any{
		"component":  componentID.String(),
		"mode":       "run",
		"config":     configID.String(),
		"configData": configData,
	}
	result := &Job{}
	request := newRequest().
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("jobs").
		WithJSONBody(data)
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
			return fmt.Errorf("job failed: %v", job.Result.Message)
		}

		// Wait and check again
		delay := retry.NextBackOff()
		if delay == backoff.Stop {
			return fmt.Errorf("timeout while waiting for the component job %s to complete", job.ID)
		}
		time.Sleep(delay)
	}
}

// newBackoff creates retry for waitForJob.
func newJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 3 * time.Second
	b.Multiplier = 2
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 5 * time.Minute
	b.Reset()
	return b
}
