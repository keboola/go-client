package jobsqueueapi

import (
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

// Job is a component job.
type Job struct {
	JobKey
	Status     string         `json:"status"`
	IsFinished bool           `json:"isFinished"`
	URL        string         `json:"url"`
	Results    map[string]any `json:"results"`
	CreateTime client.Time    `json:"createdTime"`
	StartTime  *client.Time   `json:"startTime"`
	EndTime    *client.Time   `json:"endTime"`
}
