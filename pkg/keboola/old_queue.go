package keboola

import (
	"fmt"
	"net/http"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/relvacode/iso8601"
)

const (
	OldQueueJobStatusWaiting     string = "waiting"
	OldQueueJobStatusProcessing  string = "processing"
	OldQueueJobStatusSuccess     string = "success"
	OldQueueJobStatusCancelled   string = "cancelled"
	OldQueueJobStatusError       string = "error"
	OldQueueJobStatusWarning     string = "warning"
	OldQueueJobStatusTerminating string = "terminating"
	OldQueueJobStatusTerminated  string = "terminated"
)

type ProjectDetail struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type TokenDetail struct {
	Id          string `json:"id"`
	Description string `json:"description"`
}

type OldQueueJobResult struct {
	ExceptionID string `json:"exceptionId"`
	Messages    string `json:"message"`
}

type ProcessDetail struct {
	Host string `json:"host"`
	PID  int    `json:"pid"`
}

type JobMetrics struct {
	Network NetworkMetrics `json:"network"`
	Storage StorageMetrics `json:"storage"`
	Backend string         `json:"backend"`
}

type NetworkMetrics struct {
	InBytes  uint64 `json:"inBytes"`
	OutBytes uint64 `json:"outBytes"`
}

type StorageMetrics struct {
	InBytes  uint64 `json:"inBytes"`
	OutBytes uint64 `json:"outBytes"`
}

type JobDetail struct {
	Id              JobID                  `json:"id"`
	RunID           string                 `json:"runId"`
	LockName        string                 `json:"lockName"`
	Project         ProjectDetail          `json:"project"`
	Token           TokenDetail            `json:"token"`
	Component       ComponentID            `json:"component"`
	Command         string                 `json:"command"`
	Params          *orderedmap.OrderedMap `json:"params"`
	Result          OldQueueJobResult      `json:"result"`
	Status          string                 `json:"status"`
	Process         ProcessDetail          `json:"process"`
	CreatedTime     iso8601.Time           `json:"createdTime"`
	StartTime       iso8601.Time           `json:"startTime"`
	EndTime         iso8601.Time           `json:"endTime"`
	DurationSeconds uint64                 `json:"durationSeconds"`
	WaitSeconds     uint64                 `json:"waitSeconds"`
	Metrics         *JobMetrics            `json:"metrics"`
}

type CreateJobResult struct {
	Id     JobID  `json:"id"`
	Url    string `json:"url"`
	Status string `json:"status"`
}

type oldQueueJobConfig struct {
	ImageTag           string              `json:"-"`
	Branch             BranchID            `json:"-"`
	Component          ComponentID         `json:"-"`
	Config             ConfigID            `json:"config"`
	Row                RowID               `json:"row,omitempty"`
	ConfigData         map[string]any      `json:"configData,omitempty"`
	VariableValuesID   string              `json:"variableValuesID,omitempty"`
	VariableValuesData *VariableValuesData `json:"variableValuesData,omitempty"`
}

type VariableValuesData struct {
	Values []VariableData `json:"values"`
}

type VariableData struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type OldQueueJobOption func(c *oldQueueJobConfig)

func WithBranchID(id BranchID) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.Branch = id
	}
}

func WithImageTag(tag string) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.ImageTag = tag
	}
}

func WithRowID(id RowID) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.Row = id
	}
}

func WithConfigData(configData map[string]any) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.ConfigData = configData
	}
}

func WithVariableValuesID(id string) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.VariableValuesID = id
	}
}

func WithVariableValuesData(data VariableValuesData) OldQueueJobOption {
	return func(c *oldQueueJobConfig) {
		c.VariableValuesData = &data
	}
}

// Deprecated: CreateOldQueueJobRequest is deprecated because the old queue should no longer be used.
// See https://changelog.keboola.com/2021-11-10-what-is-new-queue/ for information on how to migrate your project.
//
// CreateOldQueueJobRequest https://kebooladocker.docs.apiary.io/#reference/run/create-a-job/run-job
func (a *API) CreateOldQueueJobRequest(
	componentID ComponentID,
	configID ConfigID,
	opts ...OldQueueJobOption,
) client.APIRequest[*CreateJobResult] {
	config := initOldQueueJobConfig(componentID, configID, opts...)
	result := &CreateJobResult{}
	request := a.newRequest(SyrupAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL(config.getURL()).
		WithJSONBody(config)
	return client.NewAPIRequest(result, request)
}

type getOldQueueJobConfig struct {
	includeMetrics bool
}

type GetOldQueueJobOption func(c *getOldQueueJobConfig)

func WithMetrics() GetOldQueueJobOption {
	return func(c *getOldQueueJobConfig) {
		c.includeMetrics = true
	}
}

// Deprecated: GetOldQueueJobRequest is deprecated because the old queue should no longer be used.
// See https://changelog.keboola.com/2021-11-10-what-is-new-queue/ for information on how to migrate your project.
//
// GetOldQueueJobRequest https://syrupqueue.docs.apiary.io/#reference/jobs/job/view-job-detail
func (a *API) GetOldQueueJobRequest(
	jobID JobID,
	opts ...GetOldQueueJobOption,
) client.APIRequest[*JobDetail] {
	config := getOldQueueJobConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	result := &JobDetail{}
	request := a.newRequest(SyrupAPI).
		WithResult(result).
		WithGet("queue/jobs/{job}").
		AndPathParam("job", jobID.String())
	if config.includeMetrics {
		request = request.AndQueryParam("include", "metrics")
	}
	return client.NewAPIRequest(result, request)
}

func initOldQueueJobConfig(
	componentID ComponentID,
	configID ConfigID,
	opts ...OldQueueJobOption,
) oldQueueJobConfig {
	config := oldQueueJobConfig{Component: componentID, Config: configID}
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

func (c oldQueueJobConfig) getURL() string {
	out := "docker"

	if c.Branch > 0 {
		out += fmt.Sprintf("/branch/%s", c.Branch.String())
	}

	out += fmt.Sprintf("/%s/run", c.Component.String())

	if len(c.ImageTag) > 0 {
		out += fmt.Sprintf("/tag/%s", c.ImageTag)
	}

	return out
}
