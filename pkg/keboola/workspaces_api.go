package keboola

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/keboola/go-client/pkg/request"
)

type WorkspaceID string

func (v WorkspaceID) String() string {
	return string(v)
}

type Workspace struct {
	ID       WorkspaceID    `json:"id"`
	Type     string         `json:"type"`
	Size     string         `json:"size"` // Only exists for container workspaces (Python, R)
	Active   bool           `json:"active"`
	Shared   bool           `json:"shared"`
	User     string         `json:"user"`
	Host     string         `json:"host"`
	URL      string         `json:"url"`
	Password string         `json:"password"`
	Created  WorkspacesTime `json:"createdTimestamp"`
	Updated  WorkspacesTime `json:"updatedTimestamp"`
	Start    WorkspacesTime `json:"startTimestamp"`
	// Workspace details - only exists for Snowflake workspaces
	Details *WorkspaceDetails `json:"workspaceDetails"`
}

type WorkspaceDetails struct {
	Connection struct {
		Database  string `json:"database"`
		Schema    string `json:"schema"`
		Warehouse string `json:"warehouse"`
	} `json:"connection"`
}

func (a *AuthorizedAPI) GetWorkspaceInstanceRequest(workspaceID WorkspaceID) request.APIRequest[*Workspace] {
	result := &Workspace{}
	req := a.newRequest(WorkspacesAPI).
		WithResult(&result).
		WithGet("sandboxes/{sandboxId}").
		AndPathParam("sandboxId", workspaceID.String())
	return request.NewAPIRequest(result, req)
}

func (a *AuthorizedAPI) ListWorkspaceInstancesRequest() request.APIRequest[*[]*Workspace] {
	result := make([]*Workspace, 0)
	req := a.newRequest(WorkspacesAPI).
		WithResult(&result).
		WithGet("sandboxes")
	return request.NewAPIRequest(&result, req)
}

func (a *AuthorizedAPI) CleanWorkspaceInstances(ctx context.Context) error {
	instances, err := a.ListWorkspaceInstancesRequest().Send(ctx)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}

	for _, s := range *instances {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := a.DeleteWorkspaceJobRequest(s.ID).SendOrErr(ctx); e != nil {
				m.Lock()
				defer m.Unlock()
				err = multierror.Append(err, e)
			}
		}()
	}

	wg.Wait()
	if err != nil {
		return err
	}

	return nil
}
