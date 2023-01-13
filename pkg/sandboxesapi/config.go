package sandboxesapi

import (
	"fmt"

	"github.com/keboola/go-client/pkg/client"
)

func GetSandboxID(c *keboola.Config) (SandboxID, error) {
	id, found, err := c.Content.GetNested("parameters.id")
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("config is missing sandboxId")
	}

	out, ok := id.(string)
	if !ok {
		return "", fmt.Errorf("config.parameters.id is not a string")
	}

	return SandboxID(out), nil
}

func GetConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[*keboola.Config] {
	key := keboola.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	}
	return keboola.GetConfigRequest(key)
}

func ListConfigRequest(branchId BranchID) client.APIRequest[*[]*keboola.Config] {
	return keboola.ListConfigRequest(branchId, Component)
}

func CreateConfigRequest(branchId BranchID, name string) client.APIRequest[*keboola.ConfigWithRows] {
	config := &keboola.ConfigWithRows{
		Config: &keboola.Config{
			ConfigKey: keboola.ConfigKey{
				BranchID:    branchId,
				ComponentID: Component,
			},
			Name: name,
		},
	}
	return keboola.CreateConfigRequest(config)
}

func DeleteConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[client.NoResult] {
	request := keboola.DeleteConfigRequest(keboola.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	})
	return client.NewAPIRequest(client.NoResult{}, request)
}
