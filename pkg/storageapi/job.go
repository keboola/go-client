package storageapi

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"

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
	CreateTime      Time           `json:"createdTime"`
	StartTime       *Time          `json:"startTime"`
	EndTime         *Time          `json:"endTime"`
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

// waitForJob pulls job status until it is completed.
func waitForJob(ctx context.Context, sender client.Sender, job *Job) error {
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
			return fmt.Errorf("job failed: %v", job.Results)
		}

		// Wait and check again
		delay := retry.NextBackOff()
		if delay == backoff.Stop {
			return fmt.Errorf("timeout while waiting for the storage job %d to complete", job.ID)
		}
		time.Sleep(delay)
	}
}

// newBackoff creates retry for waitForJob.
func newJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 60 * time.Second
	b.Reset()
	return b
}
