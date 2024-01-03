package keboola

import "github.com/keboola/go-client/pkg/request"

// GetTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/table-detail
func (a *AuthorizedAPI) GetTableRequest(k TableKey) request.APIRequest[*Table] {
	bucketKey := BucketKey{BranchID: k.BranchID, BucketID: k.TableID.BucketID}
	table := &Table{TableKey: k, Bucket: &Bucket{BucketKey: bucketKey}}
	req := a.
		newRequest(StorageAPI).
		WithResult(table).
		WithGet("branch/{branchId}/tables/{tableId}").
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("tableId", k.TableID.String())
	return request.NewAPIRequest(table, req)
}
