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

type SandboxID string

func (v SandboxID) String() string {
	return string(v)
}

type Sandbox struct {
	ID       SandboxID `json:"id"`
	Type     string    `json:"type"`
	Size     string    `json:"size"` // Only exists for container sandboxes (Python, R)
	Active   bool      `json:"active"`
	Shared   bool      `json:"shared"`
	User     string    `json:"user"`
	Host     string    `json:"host"`
	Url      string    `json:"url"`
	Password string    `json:"password"`
	Created  Time      `json:"createdTimestamp"`
	Updated  Time      `json:"updatedTimestamp"`
	Start    Time      `json:"startTimestamp"`
	// Workspace details - only exists for Snowflake sandboxes
	Details *Details `json:"workspaceDetails"`
}

type Details struct {
	Connection struct {
		Database  string `json:"database"`
		Schema    string `json:"schema"`
		Warehouse string `json:"warehouse"`
	} `json:"connection"`
}

type SandboxWithConfig struct {
	Sandbox *Sandbox
	Config  *storageapi.Config
}

const Component = "keboola.sandboxes"

const (
	SizeSmall  = "small"
	SizeMedium = "medium"
	SizeLarge  = "large"
)

var SizesOrdered = []string{
	SizeSmall,
	SizeMedium,
	SizeLarge,
}

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

var TypesOrdered = []string{
	TypeSnowflake,
	TypePython,
	TypeR,
}

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

func GetSandboxID(c *storageapi.Config) (SandboxID, error) {
	id, found, err := c.Content.GetNested("parameters.id")
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("config is missing sandboxId")
	}

	out, ok := id.(string)
	if !ok {
		return "", fmt.Errorf("config.parameters.id is not a string")
	}

	return SandboxID(out), nil
}

func Create(
	ctx context.Context,
	sapiClient client.Sender,
	queueClient client.Sender,
	sandboxClient client.Sender,
	branchId BranchID,
	sandboxName string,
	sandboxType string,
	opts ...Option,
) (*SandboxWithConfig, error) {
	// Create sandbox config
	emptyConfig, err := CreateConfigRequest(branchId, sandboxName).Send(ctx, sapiClient)
	if err != nil {
		return nil, err
	}

	// Create sandbox from config
	_, err = CreateJobRequest(emptyConfig.ID, sandboxType, opts...).Send(ctx, queueClient)
	if err != nil {
		return nil, err
	}

	// Get sandbox
	sandbox, err := Get(ctx, sapiClient, sandboxClient, branchId, emptyConfig.ID)
	if err != nil {
		return nil, err
	}

	return sandbox, nil
}

func Delete(
	ctx context.Context,
	sapiClient client.Sender,
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
	_, err = DeleteConfigRequest(branchId, configId).Send(ctx, sapiClient)
	if err != nil {
		return err
	}

	return nil
}

func Get(
	ctx context.Context,
	sapiClient client.Sender,
	sandboxClient client.Sender,
	branchId BranchID,
	configId ConfigID,
) (*SandboxWithConfig, error) {
	config, err := GetConfigRequest(branchId, configId).Send(ctx, sapiClient)
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
	sapiClient client.Sender,
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
		data, err := ListConfigRequest(branchId).Send(ctx, sapiClient)
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
		m := make(map[string]*Sandbox, 0)
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
	out := make([]*SandboxWithConfig, len(configs))
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

func GetInstanceRequest(sandboxId SandboxID) client.APIRequest[*Sandbox] {
	result := &Sandbox{}
	request := newRequest().
		WithResult(&result).
		WithGet("sandboxes/{sandboxId}").
		AndPathParam("sandboxId", sandboxId.String())
	return client.NewAPIRequest(result, request)
}

func ListInstancesRequest() client.APIRequest[*[]*Sandbox] {
	result := make([]*Sandbox, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("sandboxes")
	return client.NewAPIRequest(&result, request)
}
