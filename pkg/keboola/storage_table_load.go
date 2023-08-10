package keboola

import (
	"github.com/keboola/go-client/pkg/request"
)

// LoadDataFromFileRequest https://keboola.docs.apiary.io/#reference/tables/load-data-asynchronously/import-data
func (a *API) LoadDataFromFileRequest(k TableKey, dataFileID int, opts ...LoadDataOption) request.APIRequest[*StorageJob] {
	c := &loadDataConfig{}
	for _, o := range opts {
		o.applyLoadDataOption(c)
	}

	params := request.StructToMap(c, nil)
	params["dataFileId"] = dataFileID

	job := &StorageJob{}
	req := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithPost("branch/{branchId}/tables/{tableId}/import-async").
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("tableId", k.TableID.String()).
		WithFormBody(request.ToFormBody(params))

	return request.NewAPIRequest(job, req)
}
