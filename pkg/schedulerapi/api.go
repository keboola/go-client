// Package schedulerapi contains request definitions for the Scheduler API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host and "X-StorageApi-Token" header in the HTTP client, see the ClientWithHostAndToken function.
package schedulerapi

import (
	"net/http"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
)

// ClientWithHostAndToken returns HTTP client with api host set.
func ClientWithHostAndToken(c client.Client, apiHost, apiToken string) client.Client {
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return c.WithBaseURL(`https://`+apiHost).WithHeader("X-StorageApi-Token", apiToken)
}

func newRequest() client.HTTPRequest {
	// Create request and set default error type
	return client.NewHTTPRequest().WithError(&Error{})
}

// ActivateScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/activate
func ActivateScheduleRequest(configID storageapi.ConfigID, configurationVersionID string) client.APIRequest[*Schedule] {
	body := map[string]string{
		"configurationId": configID.String(),
	}
	if configurationVersionID != "" {
		body["configurationVersionId"] = configurationVersionID
	}
	result := &Schedule{}
	request := newRequest().
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("schedules").
		WithJSONBody(body)
	return client.NewAPIRequest(result, request)
}

// DeleteScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedule
func DeleteScheduleRequest(key ScheduleKey) client.APIRequest[client.NoResult] {
	request := newRequest().
		WithMethod(http.MethodDelete).
		WithURL("schedules/{scheduleId}").
		AndPathParam("scheduleId", key.ID.String())
	return client.NewAPIRequest(client.NoResult{}, request)
}

// DeleteSchedulesForConfigurationRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedulesForConfiguration
func DeleteSchedulesForConfigurationRequest(configID ConfigID) client.APIRequest[client.NoResult] {
	request := newRequest().
		WithMethod(http.MethodDelete).
		WithURL("configurations/{configurationId}").
		AndPathParam("configurationId", configID.String())
	return client.NewAPIRequest(client.NoResult{}, request)
}

// ListSchedulesRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
func ListSchedulesRequest() client.APIRequest[*[]*Schedule] {
	result := make([]*Schedule, 0)
	request := newRequest().
		WithResult(&result).
		WithMethod(http.MethodGet).
		WithURL("schedules")
	return client.NewAPIRequest(&result, request)
}
