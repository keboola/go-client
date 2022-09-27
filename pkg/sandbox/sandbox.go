package sandbox

import (
	"context"
	"fmt"

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
	Details *struct {
		Connection struct {
			Database  string `json:"database"`
			Schema    string `json:"schema"`
			Warehouse string `json:"warehouse"`
		} `json:"connection"`
	} `json:"workspaceDetails"`
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
	branchId BranchID,
	sandboxName string,
	sandboxType string,
	opts ...Option,
) (*storageapi.Config, error) {
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

	// Get sandbox config
	// The initial config does not have the sandbox id, because the sandbox has not been created yet,
	// so we need to fetch the sandbox config after the sandbox create job finishes.
	// The sandbox id is separate from the sandbox config id, and we need both to delete the sandbox.
	config, err := GetConfigRequest(branchId, emptyConfig.ID).Send(ctx, sapiClient)
	if err != nil {
		return nil, err
	}

	return config, nil
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

func GetRequest(sandboxId SandboxID) client.APIRequest[*Sandbox] {
	sandbox := &Sandbox{}
	request := newRequest().
		WithResult(sandbox).
		WithGet("{sandboxId}").
		AndPathParam("sandboxId", sandboxId.String())
	return client.NewAPIRequest(sandbox, request)
}
