package storageapi

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"golang.org/x/sync/semaphore"
)

const mainBranchDescription = ""

// CleanProjectRequest cleans the whole project, the default branch is reset to the default state and other branches are deleted.
// Useful for E2E tests. Result is default branch.
func CleanProjectRequest() client.APIRequest[*Branch] {
	// Only one delete branch request can run simultaneously.
	// Branch deletion is performed via Storage Job, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	deleteBranchSem := semaphore.NewWeighted(1)

	// For each branch
	defaultBranch := &Branch{}
	request := ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*Branch) error {
			wg := client.NewWaitGroup(ctx, sender)
			for _, branch := range *result {
				branch := branch
				// Clear branch
				if branch.IsDefault {
					// Default branch cannot be deleted
					// Reset description
					if branch.Description != mainBranchDescription {
						branch.Description = mainBranchDescription
						wg.Send(UpdateBranchRequest(branch, []string{"description"}))
					}
					// Store default branch
					*defaultBranch = *branch
					// Clear configs
					wg.Send(DeleteConfigsInBranchRequest(branch.BranchKey))
					// Clear metadata
					wg.Send(ListBranchMetadataRequest(branch.BranchKey).
						WithOnSuccess(func(ctx context.Context, sender client.Sender, result *MetadataDetails) error {
							wgMetadata := client.NewWaitGroup(ctx, sender)
							for _, item := range *result {
								wgMetadata.Send(DeleteBranchMetadataRequest(branch.BranchKey, item.ID))
							}
							return wgMetadata.Wait()
						}))
				} else {
					// If it is not default branch -> delete branch.
					wg.Send(DeleteBranchRequest(branch.BranchKey).
						WithBefore(func(ctx context.Context, _ client.Sender) error {
							return deleteBranchSem.Acquire(ctx, 1)
						}).
						WithOnComplete(func(_ context.Context, _ client.Sender, _ client.NoResult, err error) error {
							deleteBranchSem.Release(1)
							return err
						}),
					)
				}
			}
			return wg.Wait()
		})
	return client.NewAPIRequest(defaultBranch, request)
}
