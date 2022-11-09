package storageapi

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

// JobID is an ID of a storage job.
type JobID int

func (id JobID) String() string {
	return strconv.Itoa(int(id))
}

// JobKey is a unique identifier of Job.
type JobKey struct {
	ID JobID `json:"id"`
}

// Job is a storage job.
type Job struct {
	JobKey
	Status          string         `json:"status"`
	URL             string         `json:"url"`
	OperationName   string         `json:"operationName"`
	OperationParams map[string]any `json:"operationParams"`
	Results         map[string]any `json:"results"`
	CreateTime      iso8601.Time   `json:"createdTime"`
	StartTime       *iso8601.Time  `json:"startTime"`
	EndTime         *iso8601.Time  `json:"endTime"`
}

// GetJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
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
		if job.Status == "success" {
			return nil
		} else if job.Status == "error" {
			return fmt.Errorf(`job "%s" failed: %v`, job.ID, job.Results)
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

// newBackoff creates retry for WaitForJob.
func newJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 0 // no limit, run until context timeout
	b.Reset()
	return b
}
