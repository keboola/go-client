package sandboxesapi

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
)

type BranchID = storageapi.BranchID
type ConfigID = storageapi.ConfigID

type SandboxWithConfig struct {
	Sandbox *Sandbox
	Config  *storageapi.Config
}

func (v SandboxWithConfig) String() string {
	if SupportsSizes(v.Sandbox.Type) {
		return fmt.Sprintf("ID: %s, Type: %s, Size: %s, Name: %s", v.Sandbox.ID, v.Sandbox.Type, v.Sandbox.Size, v.Config.Name)
	} else {
		return fmt.Sprintf("ID: %s, Type: %s, Name: %s", v.Sandbox.ID, v.Sandbox.Type, v.Config.Name)
	}
}

const Component = "keboola.sandboxes"

const (
	SizeSmall  = "small"
	SizeMedium = "medium"
	SizeLarge  = "large"
)

//nolint:gochecknoglobals
var SizesOrdered = []string{
	SizeSmall,
	SizeMedium,
	SizeLarge,
}

//nolint:gochecknoglobals
var SizesMap = map[string]bool{
	SizeSmall:  true,
	SizeMedium: true,
	SizeLarge:  true,
}

const (
	TypeSnowflake = "snowflake"
	TypePython    = "python"
	TypeR         = "r"
)

//nolint:gochecknoglobals
var TypesOrdered = []string{
	TypeSnowflake,
	TypePython,
	TypeR,
}

//nolint:gochecknoglobals
var TypesMap = map[string]bool{
	TypeSnowflake: true,
	TypePython:    true,
	TypeR:         true,
}

func SupportsSizes(typ string) bool {
	switch typ {
	case TypePython:
		return true
	case TypeR:
		return true
	default:
		return false
	}
}

func Create(
	ctx context.Context,
	storageClient client.Sender,
	queueClient client.Sender,
	sandboxClient client.Sender,
	branchId BranchID,
	sandboxName string,
	sandboxType string,
	opts ...Option,
) (*SandboxWithConfig, error) {
	// Create sandbox config
	emptyConfig, err := CreateConfigRequest(branchId, sandboxName).Send(ctx, storageClient)
	if err != nil {
		return nil, err
	}

	// Create sandbox from config
	_, err = CreateJobRequest(emptyConfig.ID, sandboxType, opts...).Send(ctx, queueClient)
	if err != nil {
		return nil, err
	}

	// Get sandbox
	sandbox, err := Get(ctx, storageClient, sandboxClient, branchId, emptyConfig.ID)
	if err != nil {
		return nil, err
	}

	return sandbox, nil
}

func Delete(
	ctx context.Context,
	storageClient client.Sender,
	queueClient client.Sender,
	branchId BranchID,
	configId ConfigID,
	sandboxId SandboxID,
) error {
	// Delete sandbox (this stops the instance and deletes it)
	_, err := DeleteJobRequest(configId, sandboxId).Send(ctx, queueClient)
	if err != nil {
		return err
	}

	// Delete sandbox config (so it is no longer visible in UI)
	_, err = DeleteConfigRequest(branchId, configId).Send(ctx, storageClient)
	if err != nil {
		return err
	}

	return nil
}

func Get(
	ctx context.Context,
	storageClient client.Sender,
	sandboxClient client.Sender,
	branchId BranchID,
	configId ConfigID,
) (*SandboxWithConfig, error) {
	config, err := GetConfigRequest(branchId, configId).Send(ctx, storageClient)
	if err != nil {
		return nil, err
	}

	sandboxId, err := GetSandboxID(config)
	if err != nil {
		return nil, err
	}

	sandbox, err := GetInstanceRequest(sandboxId).Send(ctx, sandboxClient)
	if err != nil {
		return nil, err
	}

	out := &SandboxWithConfig{
		Sandbox: sandbox,
		Config:  config,
	}
	return out, nil
}

func List(
	ctx context.Context,
	storageClient client.Sender,
	sandboxClient client.Sender,
	branchId BranchID,
) ([]*SandboxWithConfig, error) {
	// List configs and instances in parallel
	var configs []*storageapi.Config
	var instances map[string]*Sandbox
	errors := make(chan error, 2)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := ListConfigRequest(branchId).Send(ctx, storageClient)
		if err != nil {
			errors <- err
			return
		}
		configs = *data
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := ListInstancesRequest().Send(ctx, sandboxClient)
		if err != nil {
			errors <- err
			return
		}
		m := make(map[string]*Sandbox, len(*data))
		for _, sandbox := range *data {
			m[sandbox.ID.String()] = sandbox
		}
		instances = m
	}()

	wg.Wait()

	// Collect errors
	close(errors)
	var err error
	for e := range errors {
		err = multierror.Append(err, e)
	}
	if err != nil {
		return nil, err
	}

	// Combine config and instance lists
	out := make([]*SandboxWithConfig, 0)
	for _, config := range configs {
		sandboxId, err := GetSandboxID(config)
		if err != nil {
			// invalid configurations are ignored
			continue
		}

		out = append(out, &SandboxWithConfig{
			Sandbox: instances[sandboxId.String()],
			Config:  config,
		})
	}
	return out, nil
}
