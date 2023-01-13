package keboola

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
)

// CleanAllSchedulesRequest cleans all schedules in whole project.
// Useful for E2E tests.
func (a *API) CleanAllSchedulesRequest() client.APIRequest[client.NoResult] {
	request := a.ListSchedulesRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*Schedule) error {
			wg := client.NewWaitGroup(ctx)
			for _, schedule := range *result {
				wg.Send(a.DeleteScheduleRequest(schedule.ScheduleKey))
			}
			return wg.Wait()
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
