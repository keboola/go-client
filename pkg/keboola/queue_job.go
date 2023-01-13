package keboola

import (
	jsonLib "encoding/json"

	"github.com/relvacode/iso8601"
)

// JobID is an ID of a component job.
type JobID string

func (j JobID) String() string {
	return string(j)
}

// JobKey is a unique identifier of QueueJob.
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

// QueueJob is a component job.
type QueueJob struct {
	JobKey
	Status     string        `json:"status"`
	IsFinished bool          `json:"isFinished"`
	URL        string        `json:"url"`
	Result     JobResult     `json:"result,omitempty"`
	CreateTime iso8601.Time  `json:"createdTime"`
	StartTime  *iso8601.Time `json:"startTime"`
	EndTime    *iso8601.Time `json:"endTime"`
}
