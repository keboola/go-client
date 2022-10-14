package storageapi

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/go-client/pkg/client"
)

// RowID is id of configuration row.
type RowID string

func (v RowID) String() string {
	return string(v)
}

// ConfigRowKey is a unique identifier of ConfigRow.
type ConfigRowKey struct {
	BranchID    BranchID    `json:"-"`
	ComponentID ComponentID `json:"-"`
	ConfigID    ConfigID    `json:"-"`
	ID          RowID       `json:"id" writeas:"rowId" writeoptional:"true"`
}

func (k ConfigRowKey) ObjectId() any {
	return k.ID
}

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	ConfigRowKey
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled"`
	Version           int                    `json:"version" readonly:"true"`
	State             *orderedmap.OrderedMap `json:"state" readonly:"true"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
}

// GetConfigRowRequest https://kebooldocs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/row-detail
func (a *Api) GetConfigRowRequest(key ConfigRowKey) client.APIRequest[*ConfigRow] {
	row := &ConfigRow{}
	row.BranchID = key.BranchID
	row.ComponentID = key.ComponentID
	row.ConfigID = key.ConfigID
	request := a.
		newRequest(StorageAPI).
		WithResult(row).
		WithGet("branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		AndPathParam("branchId", key.BranchID.String()).
		AndPathParam("componentId", key.ComponentID.String()).
		AndPathParam("configId", key.ConfigID.String()).
		AndPathParam("rowId", key.ID.String())
	return client.NewAPIRequest(row, request)
}

// CreateConfigRowRequest https://kebooldocs.apiary.io/#reference/components-and-configurations/create-or-list-configuration-rows/create-development-branch-configuration-row
func (a *Api) CreateConfigRowRequest(row *ConfigRow) client.APIRequest[*ConfigRow] {
	// Create request
	request := a.
		newRequest(StorageAPI).
		WithResult(row).
		WithPost("branch/{branchId}/components/{componentId}/configs/{configId}/rows").
		AndPathParam("branchId", row.BranchID.String()).
		AndPathParam("componentId", string(row.ComponentID)).
		AndPathParam("configId", string(row.ConfigID)).
		WithFormBody(client.ToFormBody(client.StructToMap(row, nil)))
	return client.NewAPIRequest(row, request)
}

// UpdateConfigRowRequest https://kebooldocs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row-for-development-branch
func (a *Api) UpdateConfigRowRequest(row *ConfigRow, changedFields []string) client.APIRequest[*ConfigRow] {
	// ID is required
	if row.ID == "" {
		panic("config row id must be set")
	}

	// Create request
	request := a.
		newRequest(StorageAPI).
		WithResult(row).
		WithPut("branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		AndPathParam("branchId", row.BranchID.String()).
		AndPathParam("componentId", string(row.ComponentID)).
		AndPathParam("configId", string(row.ConfigID)).
		AndPathParam("rowId", string(row.ID)).
		WithFormBody(client.ToFormBody(client.StructToMap(row, changedFields)))
	return client.NewAPIRequest(row, request)
}

// DeleteConfigRowRequest https://kebooldocs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row
func (a *Api) DeleteConfigRowRequest(key ConfigRowKey) client.APIRequest[client.NoResult] {
	request := a.
		newRequest(StorageAPI).
		WithDelete("branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		AndPathParam("branchId", key.BranchID.String()).
		AndPathParam("componentId", string(key.ComponentID)).
		AndPathParam("configId", string(key.ConfigID)).
		AndPathParam("rowId", string(key.ID))
	return client.NewAPIRequest(client.NoResult{}, request)
}
