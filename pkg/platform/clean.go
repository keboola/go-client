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
	sandboxClient client.Sender,
) error {
	wg := &sync.WaitGroup{}
	errors := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := storageapi.CleanProjectRequest().SendOrErr(ctx, storageClient); err != nil {
			errors <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := schedulerapi.CleanAllSchedulesRequest().SendOrErr(ctx, schedulerClient); err != nil {
			errors <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sandboxesapi.CleanInstancesRequest().SendOrErr(ctx, sandboxClient); err != nil {
			errors <- err
		}
	}()

	wg.Wait()
	close(errors)

	var err error
	for e := range errors {
		err = multierror.Append(err, e)
	}
	if err != nil {
		return err
	}

	return nil
}
