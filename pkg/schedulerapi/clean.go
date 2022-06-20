package schedulerapi

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
)

// CleanAllSchedulesRequest cleans all schedules in whole project.
// Useful for E2E tests.
func CleanAllSchedulesRequest() client.APIRequest[client.NoResult] {
	request := ListSchedulesRequest().
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*Schedule) error {
			wg := client.NewWaitGroup(ctx, sender)
			for _, schedule := range *result {
				wg.Send(DeleteScheduleRequest(schedule.ScheduleKey))
			}
			return wg.Wait()
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
