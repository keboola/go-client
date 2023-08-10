package keboola

import "github.com/keboola/go-client/pkg/request"

// DeleteTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/drop-table
func (a *API) DeleteTableRequest(branchID BranchID, tableID TableID, opts ...DeleteOption) request.APIRequest[request.NoResult] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	req := a.
		newRequest(StorageAPI).
		WithDelete("branch/{branchId}/tables/{tableId}").
		WithOnError(ignoreResourceNotFoundError()).
		AndPathParam("branchId", branchID.String()).
		AndPathParam("tableId", tableID.String())

	if c.force {
		req = req.AndQueryParam("force", "true")
	}

	return request.NewAPIRequest(request.NoResult{}, req)
}
