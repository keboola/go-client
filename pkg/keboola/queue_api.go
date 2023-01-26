// Contains request definitions for the Jobs Queue API.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
package keboola

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/go-client/pkg/client"
)

// CreateQueueJobRequest - https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/createJob
func (a *API) CreateQueueJobRequest(componentID ComponentID, configID ConfigID) client.APIRequest[*QueueJob] {
	data := map[string]string{
		"component": componentID.String(),
		"mode":      "run",
		"config":    configID.String(),
	}
	result := QueueJob{}
	request := a.newRequest(QueueAPI).
		WithResult(&result).
		WithMethod(http.MethodPost).
		WithURL("jobs").
		WithJSONBody(data)
	return client.NewAPIRequest(&result, request)
}

// CreateQueueJobConfigDataRequest - https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/createJob
//
// Allows setting configData.
//
// `configId` can be set to an empty string, and it will be omitted.
func (a *API) CreateQueueJobConfigDataRequest(componentID ComponentID, configID ConfigID, configData map[string]any) client.APIRequest[*QueueJob] {
	body := map[string]any{
		"component":  componentID.String(),
		"mode":       "run",
		"configData": configData,
	}
	if len(configID.String()) > 0 {
		body["config"] = configID.String()
	}

	result := &QueueJob{}
	request := a.newRequest(QueueAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("jobs").
		WithJSONBody(body)
	return client.NewAPIRequest(result, request)
}

// GetQueueJobRequest https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.2#/Jobs/getJob
func (a *API) GetQueueJobRequest(key JobKey) client.APIRequest[*QueueJob] {
	return a.getQueueJobRequest(&QueueJob{JobKey: key})
}

func (a *API) getQueueJobRequest(job *QueueJob) client.APIRequest[*QueueJob] {
	request := a.newRequest(QueueAPI).
		WithResult(job).
		WithGet("jobs/{jobId}").
		AndPathParam("jobId", job.ID.String())
	return client.NewAPIRequest(job, request)
}

// WaitForQueueJob pulls job status until it is completed.
func (a *API) WaitForQueueJob(ctx context.Context, job *QueueJob) error {
	_, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("timeout for the job was not set")
	}

	retry := newQueueJobBackoff()
	for {
		// Get job status
		if err := a.getQueueJobRequest(job).SendOrErr(ctx); err != nil {
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

// newQueueJobBackoff creates retry for WaitForQueueJob.
func newQueueJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 3 * time.Second
	b.Multiplier = 2
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0 // no limit, run until context timeout
	b.Reset()
	return b
}
