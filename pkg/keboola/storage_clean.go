package keboola

import (
	"context"

	"golang.org/x/sync/semaphore"

	"github.com/keboola/go-client/pkg/client"
)

const mainBranchDescription = ""

// CleanProjectRequest cleans the whole project, the default branch is reset to the default state and other branches are deleted.
// Useful for E2E tests. Result is default branch.
func (a *API) CleanProjectRequest() client.APIRequest[*Branch] {
	// Only one delete branch request can run simultaneously.
	// Branch deletion is performed via Storage StorageJob, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	deleteBranchSem := semaphore.NewWeighted(1)

	// For each branch
	defaultBranch := &Branch{}

	cleanBranchesReq := a.
		ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*Branch) error {
			wg := client.NewWaitGroup(ctx)
			for _, branch := range *result {
				branch := branch
				// Clear branch
				if branch.IsDefault {
					// Default branch cannot be deleted
					// Reset description
					if branch.Description != mainBranchDescription {
						branch.Description = mainBranchDescription
						wg.Send(a.UpdateBranchRequest(branch, []string{"description"}))
					}
					// Store default branch
					*defaultBranch = *branch
					// Clear configs
					wg.Send(a.DeleteConfigsInBranchRequest(branch.BranchKey))
					// Clear metadata
					wg.Send(a.
						ListBranchMetadataRequest(branch.BranchKey).
						WithOnSuccess(func(ctx context.Context, result *MetadataDetails) error {
							wgMetadata := client.NewWaitGroup(ctx)
							for _, item := range *result {
								wgMetadata.Send(a.DeleteBranchMetadataRequest(branch.BranchKey, item.ID))
							}
							return wgMetadata.Wait()
						}))
				} else {
					// If it is not default branch -> delete branch.
					wg.Send(a.
						DeleteBranchRequest(branch.BranchKey).
						WithBefore(func(ctx context.Context) error {
							return deleteBranchSem.Acquire(ctx, 1)
						}).
						WithOnComplete(func(_ context.Context, _ client.NoResult, err error) error {
							deleteBranchSem.Release(1)
							return err
						}),
					)
				}
			}
			return wg.Wait()
		})

	cleanBucketsReq := a.
		ListBucketsRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*Bucket) error {
			wg := client.NewWaitGroup(ctx)
			for _, bucket := range *result {
				wg.Send(a.DeleteBucketRequest(bucket.ID, WithForce()))
			}
			return wg.Wait()
		})

	cleanFilesReq := a.
		ListFilesRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*File) error {
			wg := client.NewWaitGroup(ctx)
			for _, file := range *result {
				wg.Send(a.DeleteFileRequest(file.ID))
			}
			return wg.Wait()
		})

	cleanTokensReq := a.ListTokensRequest().
		WithOnSuccess(func(ctx context.Context, result *[]*Token) error {
			wg := client.NewWaitGroup(ctx)
			for _, token := range *result {
				if !token.IsMaster {
					wg.Send(a.DeleteTokenRequest(token.ID))
				}
			}
			return wg.Wait()
		})

	return client.NewAPIRequest(defaultBranch, client.Parallel(cleanBranchesReq, cleanBucketsReq, cleanFilesReq, cleanTokensReq))
}