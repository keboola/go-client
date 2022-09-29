package sandboxesapi

import "github.com/keboola/go-client/pkg/client"

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