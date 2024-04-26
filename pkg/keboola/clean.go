package keboola

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
)

func CleanProject(
	ctx context.Context,
	api *AuthorizedAPI,
) error {
	m := &sync.Mutex{}
	var err error

	if e := api.CleanAllSchedulesRequest().SendOrErr(ctx); e != nil {
		m.Lock()
		defer m.Unlock()
		err = multierror.Append(err, e)
	}

	if e := api.CleanWorkspaceInstances(ctx); e != nil {
		m.Lock()
		defer m.Unlock()
		err = multierror.Append(err, e)
	}

	if e := api.CleanProjectRequest().SendOrErr(ctx); e != nil {
		m.Lock()
		defer m.Unlock()
		err = multierror.Append(err, e)
	}

	if err != nil {
		return err
	}

	return nil
}
