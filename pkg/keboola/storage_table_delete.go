package keboola

import "github.com/keboola/go-client/pkg/request"

// DeleteTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/drop-table
func (a *AuthorizedAPI) DeleteTableRequest(k TableKey, opts ...DeleteOption) request.APIRequest[request.NoResult] {
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
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("tableId", k.TableID.String())

	if c.force {
		req = req.AndQueryParam("force", "true")
	}

	return request.NewAPIRequest(request.NoResult{}, req)
}
