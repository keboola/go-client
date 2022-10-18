package platform

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
)

func CleanProject(
	ctx context.Context,
	storageClient client.Sender,
	schedulerClient client.Sender,
	queueClient client.Sender,
	sandboxClient client.Sender,
) error {
	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := storageapi.CleanProjectRequest().SendOrErr(ctx, storageClient); e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := schedulerapi.CleanAllSchedulesRequest().SendOrErr(ctx, schedulerClient); e != nil {
			m.Lock()
			defer m.Unlock()
			err = multierror.Append(err, e)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := sandboxesapi.CleanInstances(ctx, queueClient, sandboxClient); e != nil {
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
