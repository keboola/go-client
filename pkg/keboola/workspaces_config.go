package keboola

import (
	"fmt"

	"github.com/keboola/go-client/pkg/request"
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

func (a *AuthorizedAPI) GetWorkspaceConfigRequest(branchID BranchID, configID ConfigID) request.APIRequest[*ConfigWithRows] {
	key := ConfigKey{
		BranchID:    branchID,
		ComponentID: WorkspacesComponent,
		ID:          configID,
	}
	return a.GetConfigRequest(key)
}

func (a *AuthorizedAPI) ListWorkspaceConfigRequest(branchID BranchID) request.APIRequest[*[]*Config] {
	return a.ListConfigRequest(branchID, WorkspacesComponent)
}

func (a *AuthorizedAPI) CreateWorkspaceConfigRequest(branchID BranchID, name string) request.APIRequest[*ConfigWithRows] {
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

func (a *AuthorizedAPI) DeleteWorkspaceConfigRequest(branchID BranchID, configID ConfigID) request.APIRequest[request.NoResult] {
	req := a.DeleteConfigRequest(ConfigKey{
		BranchID:    branchID,
		ComponentID: WorkspacesComponent,
		ID:          configID,
	})
	return request.NewAPIRequest(request.NoResult{}, req)
}
