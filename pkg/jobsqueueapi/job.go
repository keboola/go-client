package jobsqueueapi

import (
	jsonLib "encoding/json"

	"github.com/keboola/go-client/pkg/client"
)

// JobID is an ID of a component job.
type JobID string

func (j JobID) String() string {
	return string(j)
}

// JobKey is a unique identifier of Job.
type JobKey struct {
	ID JobID `json:"id"`
}

type JobResult struct {
	Error   map[string]any `json:"error,omitempty"`
	Message string         `json:"message,omitempty"`
}

// UnmarshalJSON implements JSON decoding.
func (r *JobResult) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "[]" {
		*r = JobResult{
			Error:   nil,
			Message: "",
		}
		return nil
	}
	// see https://stackoverflow.com/questions/43176625/call-json-unmarshal-inside-unmarshaljson-function-without-causing-stack-overflow
	type _r JobResult
	return jsonLib.Unmarshal(data, (*_r)(r))
}

// Job is a component job.
type Job struct {
	JobKey
	Status     string       `json:"status"`
	IsFinished bool         `json:"isFinished"`
	URL        string       `json:"url"`
	Result     JobResult    `json:"result,omitempty"`
	CreateTime client.Time  `json:"createdTime"`
	StartTime  *client.Time `json:"startTime"`
	EndTime    *client.Time `json:"endTime"`
}
