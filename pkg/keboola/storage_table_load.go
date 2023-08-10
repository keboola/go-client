package keboola

import (
	"fmt"

	"github.com/keboola/go-client/pkg/request"
)

// LoadDataFromFileRequest https://keboola.docs.apiary.io/#reference/tables/load-data-asynchronously/import-data
func (a *API) LoadDataFromFileRequest(tableKey TableKey, fileKey FileKey, opts ...LoadDataOption) request.APIRequest[*StorageJob] {
	// Check branch ID
	if tableKey.BranchID != fileKey.BranchID {
		return request.NewAPIRequest(&StorageJob{}, request.NewReqDefinitionError(
			fmt.Errorf(`table (branch:%s) and file (branch:%s) must be from the same branch`, tableKey.BranchID.String(), fileKey.BranchID.String()),
		))
	}

	c := &loadDataConfig{}
	for _, o := range opts {
		o.applyLoadDataOption(c)
	}

	params := request.StructToMap(c, nil)
	params["dataFileId"] = fileKey.FileID

	job := &StorageJob{}
	req := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithPost("branch/{branchId}/tables/{tableId}/import-async").
		AndPathParam("branchId", tableKey.BranchID.String()).
		AndPathParam("tableId", tableKey.TableID.String()).
		WithFormBody(request.ToFormBody(params))

	return request.NewAPIRequest(job, req)
}
