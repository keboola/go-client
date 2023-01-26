package keboola

import (
	"fmt"

	"github.com/keboola/go-client/pkg/client"
)

func GetWorkspaceID(c *Config) (WorkspaceID, error) {
	id, found, err := c.Content.GetNested("parameters.id")
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("config is missing parameters.id")
	}

	out, ok := id.(string)
	if !ok {
		return "", fmt.Errorf("config.parameters.id is not a string")
	}

	return WorkspaceID(out), nil
}

func (a *API) GetWorkspaceConfigRequest(branchID BranchID, configID ConfigID) client.APIRequest[*Config] {
	key := ConfigKey{
		BranchID:    branchID,
		ComponentID: WorkspacesComponent,
		ID:          configID,
	}
	return a.GetConfigRequest(key)
}

func (a *API) ListWorkspaceConfigRequest(branchID BranchID) client.APIRequest[*[]*Config] {
	return a.ListConfigRequest(branchID, WorkspacesComponent)
}

func (a *API) CreateWorkspaceConfigRequest(branchID BranchID, name string) client.APIRequest[*ConfigWithRows] {
	config := &ConfigWithRows{
		Config: &Config{
			ConfigKey: ConfigKey{
				BranchID:    branchID,
				ComponentID: WorkspacesComponent,
			},
			Name: name,
		},
	}
	return a.CreateConfigRequest(config)
}

func (a *API) DeleteWorkspaceConfigRequest(branchID BranchID, configID ConfigID) client.APIRequest[client.NoResult] {
	request := a.DeleteConfigRequest(ConfigKey{
		BranchID:    branchID,
		ComponentID: WorkspacesComponent,
		ID:          configID,
	})
	return client.NewAPIRequest(client.NoResult{}, request)
}
