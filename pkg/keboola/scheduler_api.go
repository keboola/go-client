// Contains request definitions for the Scheduler API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
package keboola

import (
	"net/http"

	"github.com/keboola/go-client/pkg/client"
)

// ActivateScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/activate
func (a *API) ActivateScheduleRequest(configID ConfigID, configurationVersionID string) client.APIRequest[*Schedule] {
	body := map[string]string{
		"configurationId": configID.String(),
	}
	if configurationVersionID != "" {
		body["configurationVersionId"] = configurationVersionID
	}
	result := &Schedule{}
	request := a.newRequest(SchedulerAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("schedules").
		WithJSONBody(body)
	return client.NewAPIRequest(result, request)
}

// DeleteScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedule
func (a *API) DeleteScheduleRequest(key ScheduleKey) client.APIRequest[client.NoResult] {
	request := a.newRequest(SchedulerAPI).
		WithMethod(http.MethodDelete).
		WithURL("schedules/{scheduleId}").
		AndPathParam("scheduleId", key.ID.String())
	return client.NewAPIRequest(client.NoResult{}, request)
}

// DeleteSchedulesForConfigurationRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedulesForConfiguration
func (a *API) DeleteSchedulesForConfigurationRequest(configID ConfigID) client.APIRequest[client.NoResult] {
	request := a.newRequest(SchedulerAPI).
		WithMethod(http.MethodDelete).
		WithURL("configurations/{configurationId}").
		AndPathParam("configurationId", configID.String())
	return client.NewAPIRequest(client.NoResult{}, request)
}

// ListSchedulesRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
func (a *API) ListSchedulesRequest() client.APIRequest[*[]*Schedule] {
	result := make([]*Schedule, 0)
	request := a.newRequest(SchedulerAPI).
		WithResult(&result).
		WithMethod(http.MethodGet).
		WithURL("schedules")
	return client.NewAPIRequest(&result, request)
}
