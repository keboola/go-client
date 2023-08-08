package keboola

import (
	jsonLib "encoding/json"

	"github.com/keboola/go-client/pkg/request"
)

type TableMetadata []MetadataDetail

type ColumnMetadata []MetadataDetail

type ColumnsMetadata map[string]ColumnMetadata

type TableMetadataRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ColumnMetadataRequest struct {
	ColumnName string `json:"columnName"`
	Key        string `json:"key"`
	Value      string `json:"value"`
}

// TableMetadataResponse https://keboola.docs.apiary.io/#reference/metadata/table-metadata/create-or-update
type TableMetadataResponse struct {
	Metadata       TableMetadata   `json:"metadata"`
	ColumnMetadata ColumnsMetadata `json:"columnsMetadata"`
}

// UnmarshalJSON implements JSON decoding.
// The API returns empty value as empty array.
func (r *ColumnsMetadata) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "[]" {
		*r = ColumnsMetadata{}
		return nil
	}
	// see https://stackoverflow.com/questions/43176625/call-json-unmarshal-inside-unmarshaljson-function-without-causing-stack-overflow
	type _r ColumnsMetadata
	return jsonLib.Unmarshal(data, (*_r)(r))
}

// CreateOrUpdateTableMetadata https://keboola.docs.apiary.io/#reference/metadata/table-metadata/create-or-update
func (a *API) CreateOrUpdateTableMetadata(k TableKey, provider string, tableMetadata []TableMetadataRequest, columnsMetadata []ColumnMetadataRequest) request.APIRequest[*TableMetadataResponse] {
	params := make(map[string]any)
	params["provider"] = provider
	if len(tableMetadata) != 0 {
		params["metadata"] = tableMetadata
	}
	if len(columnsMetadata) != 0 {
		params["columnsMetadata"] = map[string]any{
			"all": columnsMetadata,
		}
	}

	result := &TableMetadataResponse{}
	req := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithPost("branch/{branchId}/tables/{tableId}/metadata").
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("tableId", k.TableID.String()).
		WithJSONBody(params)

	return request.NewAPIRequest(result, req)
}
