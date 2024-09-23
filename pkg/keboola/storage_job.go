package keboola

import (
	"context"
	jsonLib "encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/relvacode/iso8601"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/request"
)

// StorageJobID is an ID of a storage job.
type StorageJobID int

const (
	StorageJobStatusSuccess    string = "success"
	StorageJobStatusWaiting    string = "waiting"
	StorageJobStatusProcessing string = "processing"
	StorageJobStatusError      string = "error"
)

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
func (a *AuthorizedAPI) GetStorageJobRequest(key StorageJobKey) request.APIRequest[*StorageJob] {
	return a.getStorageJobRequest(&StorageJob{StorageJobKey: key})
}

func (a *AuthorizedAPI) getStorageJobRequest(job *StorageJob) request.APIRequest[*StorageJob] {
	req := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithGet("jobs/{jobId}").
		AndPathParam("jobId", job.ID.String())
	return request.NewAPIRequest(job, req)
}

// WaitForStorageJob pulls job status until it is completed.
func (a *AuthorizedAPI) WaitForStorageJob(ctx context.Context, job *StorageJob) (err error) {
	_, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("timeout for the job was not set")
	}

	// Telemetry
	parentSpan := trace.SpanFromContext(ctx)
	var span trace.Span
	ctx, span = parentSpan.TracerProvider().Tracer(appName).Start(ctx, "keboola.go.api.client.waitFor.storageJob")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	i := 0
	startedAt := time.Now()
	retry := newStorageJobBackoff()
	for {
		// Get job status
		if err := a.getStorageJobRequest(job).SendOrErr(ctx); err != nil {
			return fmt.Errorf(`error after %d retries, total time %s: %w`, i, time.Since(startedAt), err)
		}

		// Check status
		if job.Status == StorageJobStatusSuccess {
			return nil
		} else if job.Status == StorageJobStatusError {
			return fmt.Errorf(`job "%s" failed: %s (exception id: %s)`, job.ID, job.Error.Message, job.Error.ExceptionID)
		}

		// Wait and check again
		select {
		case <-ctx.Done():
			return fmt.Errorf(`error while waiting for the job "%s" to complete: %w`, job.ID, ctx.Err())
		case <-time.After(retry.NextBackOff()):
			// try again
		}

		i += 1
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
