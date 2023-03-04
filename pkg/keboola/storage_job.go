package keboola

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

// StorageJobID is an ID of a storage job.
type StorageJobID int

func (id StorageJobID) String() string {
	return strconv.Itoa(int(id))
}

// StorageJobKey is a unique identifier of StorageJob.
type StorageJobKey struct {
	ID StorageJobID `json:"id"`
}

type StorageJobResult map[string]any

// UnmarshalJSON implements JSON decoding.
// The API returns empty array when the results field is empty.
func (r *StorageJobResult) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "[]" {
		*r = StorageJobResult{}
		return nil
	}
	// see https://stackoverflow.com/questions/43176625/call-json-unmarshal-inside-unmarshaljson-function-without-causing-stack-overflow
	type _r StorageJobResult
	return jsonLib.Unmarshal(data, (*_r)(r))
}

// StorageJob is a storage job.
type StorageJob struct {
	StorageJobKey
	Status          string           `json:"status"`
	URL             string           `json:"url"`
	OperationName   string           `json:"operationName"`
	OperationParams map[string]any   `json:"operationParams"`
	Results         StorageJobResult `json:"results,omitempty"`
	CreateTime      iso8601.Time     `json:"createdTime"`
	StartTime       *iso8601.Time    `json:"startTime"`
	EndTime         *iso8601.Time    `json:"endTime"`
	Error           *StorageJobError `json:"error,omitempty"`
}

type StorageJobError struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	ExceptionID string `json:"exceptionId"`
}

// GetStorageJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
func (a *API) GetStorageJobRequest(key StorageJobKey) client.APIRequest[*StorageJob] {
	return a.getStorageJobRequest(&StorageJob{StorageJobKey: key})
}

func (a *API) getStorageJobRequest(job *StorageJob) client.APIRequest[*StorageJob] {
	request := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithGet("jobs/{jobId}").
		AndPathParam("jobId", job.ID.String())
	return client.NewAPIRequest(job, request)
}

// WaitForStorageJob pulls job status until it is completed.
func (a *API) WaitForStorageJob(ctx context.Context, job *StorageJob) error {
	_, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("timeout for the job was not set")
	}

	retry := newStorageJobBackoff()
	for {
		// Get job status
		if err := a.getStorageJobRequest(job).SendOrErr(ctx); err != nil {
			return err
		}

		// Check status
		if job.Status == "success" {
			return nil
		} else if job.Status == "error" {
			return fmt.Errorf(`job "%s" failed: %s (exception id: %s)`, job.ID, job.Error.Message, job.Error.ExceptionID)
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

// newStorageJobBackoff creates retry for WaitForStorageJob.
func newStorageJobBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 0 // no limit, run until context timeout
	b.Reset()
	return b
}
