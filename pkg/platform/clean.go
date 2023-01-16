package platform

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/keboola/go-client/pkg/keboola"
)

func CleanProject(
	ctx context.Context,
	api *keboola.API,
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
		if e := keboola.CleanWorkspaceInstances(ctx, queueClient, sandboxClient); e != nil {
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
