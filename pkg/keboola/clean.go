package keboola

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
)

func CleanProject(
	ctx context.Context,
	api *API,
) error {
	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := api.CleanProjectRequest().SendOrErr(ctx); e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := api.CleanAllSchedulesRequest().SendOrErr(ctx); e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := api.CleanWorkspaceInstances(ctx); e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
		}
	}()

	wg.Wait()
	if err != nil {
		return err
	}

	return nil
}
