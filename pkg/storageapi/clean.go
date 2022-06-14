package storageapi

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
)

// CleanProjectRequest clear whole project, default branch is reset to default state, other branches are deleted.
// Useful for E2E tests.
func CleanProjectRequest() client.APIRequest[client.NoResult] {
	// For each branch
	request := ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*Branch) error {
			wg := client.NewWaitGroup(ctx, sender)
			for _, branch := range *result {
				branch := branch
				// Clear branch
				if branch.IsDefault {
					// Default branch cannot be deleted
					// Reset description
					if branch.Description != "" {
						branch.Description = ""
						wg.Send(UpdateBranchRequest(branch, []string{"description"}))
					}
					// Clear configs
					wg.Send(DeleteConfigsInBranchRequest(branch.BranchKey))
					// Clear metadata
					wg.Send(ListBranchMetadataRequest(branch.BranchKey).
						WithOnSuccess(func(ctx context.Context, sender client.Sender, result *MetadataDetails) error {
							wgMetadata := client.NewWaitGroup(ctx, sender)
							for _, item := range *result {
								item := item
								wgMetadata.Send(DeleteBranchMetadataRequest(branch.BranchKey, item.ID))
							}
							return wgMetadata.Wait()
						}))
				} else {
					// If it is not default branch -> delete branch.
					wg.Send(DeleteBranchRequest(branch.BranchKey))
				}
			}
			return wg.Wait()
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
