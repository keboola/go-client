package keboola

import (
	"context"

	"github.com/keboola/go-client/pkg/request"
)

// CleanAllSchedulesRequest cleans all schedules in whole project.
// Useful for E2E tests.
func (a *API) CleanAllSchedulesRequest() request.APIRequest[request.NoResult] {
	req := a.ListSchedulesRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*Schedule) error {
			wg := request.NewWaitGroup(ctx)
			for _, schedule := range *result {
				wg.Send(a.DeleteScheduleRequest(schedule.ScheduleKey))
			}
			return wg.Wait()
		})
	return request.NewAPIRequest(request.NoResult{}, req)
}
