package keboola

// The file contains request definitions for the Scheduler API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.

import (
	"net/http"

	"github.com/keboola/go-client/pkg/request"
)

// ActivateScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/activate
func (a *AuthorizedAPI) ActivateScheduleRequest(configID ConfigID, configurationVersionID string) request.APIRequest[*Schedule] {
	body := map[string]string{
		"configurationId": configID.String(),
	}
	if configurationVersionID != "" {
		body["configurationVersionId"] = configurationVersionID
	}
	result := &Schedule{}
	req := a.newRequest(SchedulerAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL(SchedulerAPISchedules).
		WithJSONBody(body)
	return request.NewAPIRequest(result, req)
}

// DeleteScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedule
func (a *AuthorizedAPI) DeleteScheduleRequest(key ScheduleKey) request.APIRequest[request.NoResult] {
	req := a.newRequest(SchedulerAPI).
		WithMethod(http.MethodDelete).
		WithURL(SchedulerAPISchedule).
		AndPathParam("scheduleId", key.ID.String())
	return request.NewAPIRequest(request.NoResult{}, req)
}

// DeleteSchedulesForConfigurationRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedulesForConfiguration
func (a *AuthorizedAPI) DeleteSchedulesForConfigurationRequest(configID ConfigID) request.APIRequest[request.NoResult] {
	req := a.newRequest(SchedulerAPI).
		WithMethod(http.MethodDelete).
		WithURL(SchedulerAPIConfigSchedules).
		AndPathParam("configurationId", configID.String())
	return request.NewAPIRequest(request.NoResult{}, req)
}

// ListSchedulesRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
func (a *AuthorizedAPI) ListSchedulesRequest() request.APIRequest[*[]*Schedule] {
	result := make([]*Schedule, 0)
	req := a.newRequest(SchedulerAPI).
		WithResult(&result).
		WithMethod(http.MethodGet).
		WithURL(SchedulerAPISchedules)
	return request.NewAPIRequest(&result, req)
}

// GetScheduleRequest retrieves a schedule by its ID
// https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/getSchedule
func (a *AuthorizedAPI) GetScheduleRequest(key ScheduleKey) request.APIRequest[*Schedule] {
	var result Schedule
	req := a.newRequest(SchedulerAPI).
		WithResult(&result).
		WithMethod(http.MethodGet).
		WithURL(SchedulerAPISchedule).
		AndPathParam("scheduleId", key.ID.String())
	return request.NewAPIRequest(&result, req)
}

// RefreshScheduleTokenRequest refreshes the token for a schedule
// https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/refreshToken
func (a *AuthorizedAPI) RefreshScheduleTokenRequest(scheduleID ScheduleID) request.APIRequest[request.NoResult] {
	req := a.newRequest(SchedulerAPI).
		WithMethod(http.MethodPost).
		WithURL(SchedulerAPIRefreshToken).
		AndPathParam("scheduleId", scheduleID.String())
	return request.NewAPIRequest(request.NoResult{}, req)
}
