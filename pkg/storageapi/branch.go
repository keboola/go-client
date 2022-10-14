package storageapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

// BranchID is an ID of a development branch in Storage API.
type BranchID int

func (id BranchID) String() string {
	return strconv.Itoa(int(id))
}

// BranchKey is a unique identifier of a branch.
type BranchKey struct {
	ID BranchID `json:"id" writeoptional:"true"`
}

func (k BranchKey) ObjectId() any {
	return k.ID
}

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	BranchKey
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Created     iso8601.Time `json:"created" readonly:"true"`
	IsDefault   bool         `json:"isDefault" readonly:"true"`
}

// ListBranchesRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
func (a *Api) ListBranchesRequest() client.APIRequest[*[]*Branch] {
	result := make([]*Branch, 0)
	request := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("dev-branches")
	return client.NewAPIRequest(&result, request)
}

// GetDefaultBranchRequest lists all branches and returns the default branch.
func (a *Api) GetDefaultBranchRequest() client.APIRequest[*Branch] {
	defaultBranch := &Branch{}
	request := a.
		ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*Branch) error {
			for _, branch := range *result {
				if branch.IsDefault {
					*defaultBranch = *branch
					return nil
				}
			}
			return fmt.Errorf("no default branch found")
		})
	return client.NewAPIRequest(defaultBranch, request)
}

// GetBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/branch-detail
func (a *Api) GetBranchRequest(key BranchKey) client.APIRequest[*Branch] {
	result := &Branch{}
	request := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithGet("dev-branches/{branchId}").
		AndPathParam("branchId", key.ID.String())
	return client.NewAPIRequest(result, request)
}

// CreateBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/create-branch
func (a *Api) CreateBranchRequest(branch *Branch) client.APIRequest[*Branch] {
	request := a.
		CreateBranchAsyncRequest(branch).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, job *Job) error {
			// Wait for storage job
			waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*1)
			defer waitCancelFn()
			if err := WaitForJob(waitCtx, sender, job); err != nil {
				return err
			}

			// Map job results to branch
			resultsBytes, err := json.Marshal(job.Results)
			if err != nil {
				return fmt.Errorf("cannot convert job.results to JSON: %w", err)
			}
			if err := json.Unmarshal(resultsBytes, branch); err != nil {
				return fmt.Errorf("cannot map job.results to branch: %w", err)
			}
			return nil
		})
	// Result is branch, not job.
	return client.NewAPIRequest(branch, request)
}

// CreateBranchAsyncRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/create-branch
func (a *Api) CreateBranchAsyncRequest(branch *Branch) client.APIRequest[*Job] {
	// ID is autogenerated
	if branch.ID != 0 {
		panic(fmt.Errorf("branch id is set but it should be auto-generated"))
	}

	// Default branch cannot be created
	if branch.IsDefault {
		panic(fmt.Errorf("default branch cannot be created"))
	}

	result := &Job{}
	request := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithPost("dev-branches").
		WithFormBody(client.ToFormBody(client.StructToMap(branch, nil)))
	return client.NewAPIRequest(result, request)
}

// UpdateBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/update-branch
func (a *Api) UpdateBranchRequest(branch *Branch, changedFields []string) client.APIRequest[*Branch] {
	// ID is required
	if branch.ID == 0 {
		panic("branch id must be set")
	}

	if branch.IsDefault {
		for _, field := range changedFields {
			if field == "name" {
				panic(fmt.Errorf("the name of the main branch cannot be changed"))
			}
		}
	}

	// Create request
	request := a.
		newRequest(StorageAPI).
		WithResult(branch).
		WithPut("dev-branches/{branchId}").
		AndPathParam("branchId", branch.ID.String()).
		WithFormBody(client.ToFormBody(client.StructToMap(branch, changedFields)))
	return client.NewAPIRequest(branch, request)
}

// DeleteBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/delete-branch
func (a *Api) DeleteBranchRequest(key BranchKey) client.APIRequest[client.NoResult] {
	request := a.
		DeleteBranchAsyncRequest(key).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, job *Job) error {
			// Wait for storage job
			waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*1)
			defer waitCancelFn()
			return WaitForJob(waitCtx, sender, job)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}

// DeleteBranchAsyncRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/delete-branch
func (a *Api) DeleteBranchAsyncRequest(key BranchKey) client.APIRequest[*Job] {
	result := &Job{}
	request := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithDelete("dev-branches/{branchId}").
		AndPathParam("branchId", key.ID.String())
	return client.NewAPIRequest(result, request)
}

// ListBranchMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/list
func (a *Api) ListBranchMetadataRequest(key BranchKey) client.APIRequest[*MetadataDetails] {
	result := make(MetadataDetails, 0)
	request := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("branch/{branchId}/metadata").
		AndPathParam("branchId", key.ID.String())
	return client.NewAPIRequest(&result, request)
}

// AppendBranchMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/create-or-update https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/delete
func (a *Api) AppendBranchMetadataRequest(key BranchKey, metadata Metadata) client.APIRequest[client.NoResult] {
	// Empty, we have nothing to append
	if len(metadata) == 0 {
		return client.NewNoOperationAPIRequest(client.NoResult{})
	}

	// Metadata with empty values must be collected and deleted separately
	toDelete := map[string]bool{}
	formBody := make(map[string]string)
	i := 0
	for k, v := range metadata {
		if v == "" {
			toDelete[k] = true
		} else {
			formBody[fmt.Sprintf("metadata[%d][key]", i)] = k
			formBody[fmt.Sprintf("metadata[%d][value]", i)] = v
			i++
		}
	}

	requestAppend := a.
		newRequest(StorageAPI).
		WithPost("branch/{branchId}/metadata").
		AndPathParam("branchId", key.ID.String()).
		WithFormBody(formBody)

	// Delete metadata with empty values
	if len(toDelete) > 0 {
		requestListDelete := a.
			ListBranchMetadataRequest(key).
			WithOnSuccess(func(ctx context.Context, sender client.Sender, details *MetadataDetails) error {
				wg := client.NewWaitGroup(ctx, sender)
				for _, detail := range *details {
					if found := toDelete[detail.Key]; found {
						wg.Send(a.DeleteBranchMetadataRequest(key, detail.ID))
					}
				}
				return wg.Wait()
			})

		if len(formBody) > 0 {
			return client.NewAPIRequest(client.NoResult{}, requestAppend, requestListDelete)
		}
		return client.NewAPIRequest(client.NoResult{}, requestListDelete)
	}

	return client.NewAPIRequest(client.NoResult{}, requestAppend)
}

// DeleteBranchMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/delete
func (a *Api) DeleteBranchMetadataRequest(branch BranchKey, metaID string) client.APIRequest[client.NoResult] {
	request := a.
		newRequest(StorageAPI).
		WithDelete("branch/{branchId}/metadata/{metadataId}").
		AndPathParam("branchId", branch.ID.String()).
		AndPathParam("metadataId", metaID)
	return client.NewAPIRequest(client.NoResult{}, request)
}
