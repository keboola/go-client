package keboola

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type WorkspaceWithConfig struct {
	Workspace *Workspace
	Config    *Config
}

func (v WorkspaceWithConfig) String() string {
	if WorkspaceSupportsSizes(v.Workspace.Type) {
		return fmt.Sprintf("ID: %s, Type: %s, Size: %s, Name: %s", v.Workspace.ID, v.Workspace.Type, v.Workspace.Size, v.Config.Name)
	} else {
		return fmt.Sprintf("ID: %s, Type: %s, Name: %s", v.Workspace.ID, v.Workspace.Type, v.Config.Name)
	}
}

const WorkspacesComponent = "keboola.sandboxes"

const (
	WorkspaceSizeSmall  = "small"
	WorkspaceSizeMedium = "medium"
	WorkspaceSizeLarge  = "large"
)

func WorkspaceSizesOrdered() []string {
	return []string{
		WorkspaceSizeSmall,
		WorkspaceSizeMedium,
		WorkspaceSizeLarge,
	}
}

func WorkspaceSizesMap() map[string]bool {
	return map[string]bool{
		WorkspaceSizeSmall:  true,
		WorkspaceSizeMedium: true,
		WorkspaceSizeLarge:  true,
	}
}

const (
	WorkspaceTypeSnowflake = "snowflake"
	WorkspaceTypePython    = "python"
	WorkspaceTypeR         = "r"
)

func WorkspaceTypesOrdered() []string {
	return []string{
		WorkspaceTypeSnowflake,
		WorkspaceTypePython,
		WorkspaceTypeR,
	}
}

func WorkspaceTypesMap() map[string]bool {
	return map[string]bool{
		WorkspaceTypeSnowflake: true,
		WorkspaceTypePython:    true,
		WorkspaceTypeR:         true,
	}
}

func WorkspaceSupportsSizes(typ string) bool {
	switch typ {
	case WorkspaceTypePython:
		return true
	case WorkspaceTypeR:
		return true
	default:
		return false
	}
}

func (a *API) CreateWorkspace(
	ctx context.Context,
	branchId BranchID,
	workspaceName string,
	workspaceType string,
	opts ...CreateWorkspaceOption,
) (*WorkspaceWithConfig, error) {
	// Create config
	emptyConfig, err := a.CreateWorkspaceConfigRequest(branchId, workspaceName).Send(ctx)
	if err != nil {
		return nil, err
	}

	// Create workspace from config
	_, err = a.CreateWorkspaceJobRequest(emptyConfig.ID, workspaceType, opts...).Send(ctx)
	if err != nil {
		return nil, err
	}

	// Get workspace
	workspace, err := a.GetWorkspace(ctx, branchId, emptyConfig.ID)
	if err != nil {
		return nil, err
	}

	return workspace, nil
}

func (a *API) DeleteWorkspace(
	ctx context.Context,
	branchID BranchID,
	configID ConfigID,
	workspaceID WorkspaceID,
) error {
	// Delete workspace (this stops the instance and deletes it)
	_, err := a.DeleteWorkspaceJobRequest(workspaceID).Send(ctx)
	if err != nil {
		return err
	}

	// Delete workspace config (so it is no longer visible in UI)
	_, err = a.DeleteWorkspaceConfigRequest(branchID, configID).Send(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (a *API) GetWorkspace(
	ctx context.Context,
	branchID BranchID,
	configID ConfigID,
) (*WorkspaceWithConfig, error) {
	config, err := a.GetWorkspaceConfigRequest(branchID, configID).Send(ctx)
	if err != nil {
		return nil, err
	}

	workspaceID, err := GetWorkspaceID(config)
	if err != nil {
		return nil, err
	}

	workspace, err := a.GetWorkspaceInstanceRequest(workspaceID).Send(ctx)
	if err != nil {
		return nil, err
	}

	out := &WorkspaceWithConfig{
		Workspace: workspace,
		Config:    config,
	}
	return out, nil
}

func (a *API) ListWorkspaces(
	ctx context.Context,
	branchId BranchID,
) ([]*WorkspaceWithConfig, error) {
	// List configs and instances in parallel
	var configs []*Config
	var instances map[string]*Workspace
	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, e := a.ListWorkspaceConfigRequest(branchId).Send(ctx)
		if e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
			return
		}
		configs = *data
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, e := a.ListWorkspaceInstancesRequest().Send(ctx)
		if e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
			return
		}
		m := make(map[string]*Workspace, len(*data))
		for _, workspace := range *data {
			m[workspace.ID.String()] = workspace
		}
		instances = m
	}()

	wg.Wait()
	if err != nil {
		return nil, err
	}

	// Combine config and instance lists
	out := make([]*WorkspaceWithConfig, 0)
	for _, config := range configs {
		workspaceID, err := GetWorkspaceID(config)
		if err != nil {
			// invalid configurations are ignored
			continue
		}

		instance, found := instances[workspaceID.String()]
		if !found {
			continue
		}

		out = append(out, &WorkspaceWithConfig{
			Workspace: instance,
			Config:    config,
		})
	}
	return out, nil
}
