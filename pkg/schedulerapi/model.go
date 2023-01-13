package schedulerapi

import (
	"github.com/keboola/go-client/pkg/keboola"
)

// ScheduleID is ID of a schedule in Scheduler API.
type ScheduleID string

func (v ScheduleID) String() string {
	return string(v)
}

// ConfigID is ID of a configuration in Storage API.
type ConfigID = keboola.ConfigID

// ScheduleKey is a unique identifier of a schedule.
type ScheduleKey struct {
	ID ScheduleID `json:"id" validate:"required"`
}

// Schedule - https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
type Schedule struct {
	ScheduleKey
	ConfigID ConfigID `json:"configurationId"`
}
