package keboola

// ScheduleID is ID of a schedule in Scheduler API.
type ScheduleID string

func (v ScheduleID) String() string {
	return string(v)
}

// ScheduleKey is a unique identifier of a schedule.
type ScheduleKey struct {
	ID ScheduleID `json:"id" validate:"required"`
}

// Schedule - https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
type Schedule struct {
	ScheduleKey
	ConfigID               ConfigID            `json:"configurationId"`
	ConfigurationVersionID string              `json:"configurationVersionId"`
	ScheduleCron           ScheduleCron        `json:"schedule"`
	ScheduleTarget         ScheduleTarget      `json:"target"`
	Executions             []ScheduleExecution `json:"executions"`
}

type ScheduleCron struct {
	CronTab  string `json:"cronTab"`
	Timezone string `json:"timezone"`
	State    string `json:"state"`
}

type ScheduleTarget struct {
	ComponentID     ComponentID `json:"componentId"`
	ConfigurationID ConfigID    `json:"configurationId"`
	Mode            string      `json:"mode"`
	Tag             string      `json:"tag"`
}

type ScheduleExecution struct {
	JobID         string `json:"jobId"`
	ExecutionTime string `json:"executionTime"`
}
