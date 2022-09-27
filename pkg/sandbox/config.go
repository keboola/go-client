package sandboxapi

import (
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
)

func GetConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[*storageapi.Config] {
	key := storageapi.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	}
	return storageapi.GetConfigRequest(key)
}

func ListConfigRequest(branchId BranchID) client.APIRequest[*[]*storageapi.Config] {
	return storageapi.ListConfigRequest(branchId, Component)
}

func CreateConfigRequest(branchId BranchID, name string) client.APIRequest[*storageapi.ConfigWithRows] {
	config := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branchId,
				ComponentID: Component,
			},
			Name: name,
		},
	}
	return storageapi.CreateConfigRequest(config)
}

func DeleteConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[client.NoResult] {
	request := storageapi.DeleteConfigRequest(storageapi.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	})
	return client.NewAPIRequest(client.NoResult{}, request)
}
