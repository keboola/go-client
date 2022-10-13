package platform

import (
	"context"

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
	if err := storageapi.CleanProjectRequest().SendOrErr(ctx, storageClient); err != nil {
		return err
	}

	if err := schedulerapi.CleanAllSchedulesRequest().SendOrErr(ctx, schedulerClient); err != nil {
		return err
	}

	if err := sandboxesapi.CleanInstancesRequest().SendOrErr(ctx, sandboxClient); err != nil {
		return err
	}

	return nil
}
