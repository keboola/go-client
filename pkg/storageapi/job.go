package storageapi

import (
	"context"
	jsonLib "encoding/json"
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

type JobResult map[string]any

// UnmarshalJSON implements JSON decoding.
// The API returns empty array when the results field is empty.
func (r *JobResult) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "[]" {
		*r = JobResult{}
		return nil
	}
	// see https://stackoverflow.com/questions/43176625/call-json-unmarshal-inside-unmarshaljson-function-without-causing-stack-overflow
	type _r JobResult
	return jsonLib.Unmarshal(data, (*_r)(r))
}

// Job is a storage job.
type Job struct {
	JobKey
	Status          string         `json:"status"`
	URL             string         `json:"url"`
	OperationName   string         `json:"operationName"`
	OperationParams map[string]any `json:"operationParams"`
	Results         JobResult      `json:"results,omitempty"`
	CreateTime      iso8601.Time   `json:"createdTime"`
	StartTime       *iso8601.Time  `json:"startTime"`
	EndTime         *iso8601.Time  `json:"endTime"`
	Error           JobError       `json:"error,omitempty"`
}

type JobError struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	ExceptionId string `json:"exceptionId"`
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
			return fmt.Errorf(`job "%s" failed: %s (exception id: %s)`, job.ID, job.Error.Message, job.Error.ExceptionId)
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
