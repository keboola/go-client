package storageapi

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
)

// Component constants
const (
	VariablesComponentID     = ComponentID(`keboola.variables`)
	SchedulerComponentID     = ComponentID("keboola.scheduler")
	SharedCodeComponentID    = ComponentID("keboola.shared-code")
	OrchestratorComponentID  = ComponentID("keboola.orchestrator")
	TransformationType       = "transformation"
	DeprecatedFlag           = `deprecated`
	ExcludeFromNewListFlag   = `excludeFromNewList`
	ComponentTypeCodePattern = `code-pattern`
	ComponentTypeProcessor   = `processor`
)

// ComponentID is id of a Keboola component.
type ComponentID string

func (v ComponentID) String() string {
	return string(v)
}

// ComponentKey is unique identifier of a component.
type ComponentKey struct {
	ID ComponentID `json:"id"`
}

// Components slice.
type Components []*Component

// Component https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type Component struct {
	ComponentKey
	Type           string                 `json:"type"`
	Name           string                 `json:"name"`
	Flags          []string               `json:"flags,omitempty"`
	Schema         json.RawMessage        `json:"configurationSchema,omitempty"`
	SchemaRow      json.RawMessage        `json:"configurationRowSchema,omitempty"`
	EmptyConfig    *orderedmap.OrderedMap `json:"emptyConfiguration,omitempty"`
	EmptyConfigRow *orderedmap.OrderedMap `json:"emptyConfigurationRow,omitempty"`
	Data           ComponentData          `json:"data"`
}

// ComponentData https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type ComponentData struct {
	DefaultBucket      bool   `json:"default_bucket"`       //nolint: tagliatelle
	DefaultBucketStage string `json:"default_bucket_stage"` //nolint: tagliatelle
}

// ComponentWithConfigs is result of ListConfigsAndRowsFrom request.
type ComponentWithConfigs struct {
	BranchID BranchID `json:"branchId"`
	Component
	Configs []*ConfigWithRows `json:"configurations"`
}

// IsTransformation returns true, if component is transformation.
func (c *Component) IsTransformation() bool {
	return c.Type == TransformationType
}

// IsSharedCode returns true, if component is shared code.
func (c *Component) IsSharedCode() bool {
	return c.ID == SharedCodeComponentID
}

// IsVariables returns true, if component has "variables" typ.
func (c *Component) IsVariables() bool {
	return c.ID == VariablesComponentID
}

// IsCodePattern returns true, if component is IsCodePattern.
func (c *Component) IsCodePattern() bool {
	return c.Type == ComponentTypeCodePattern
}

// IsProcessor returns true, if component is processor.
func (c *Component) IsProcessor() bool {
	return c.Type == ComponentTypeProcessor
}

// IsScheduler returns true, if component is scheduler.
func (c *Component) IsScheduler() bool {
	return c.ID == SchedulerComponentID
}

// IsOrchestrator returns true, if component is orchestration.
func (c *Component) IsOrchestrator() bool {
	return c.ID == OrchestratorComponentID
}

// IsDeprecated returns true, if component is deprecated.
func (c *Component) IsDeprecated() bool {
	for _, flag := range c.Flags {
		if flag == DeprecatedFlag {
			return true
		}
	}
	return false
}

// IsExcludedFromNewList returns true, if component should be excluded from list of available new components.
func (c *Component) IsExcludedFromNewList() bool {
	for _, flag := range c.Flags {
		if flag == ExcludeFromNewListFlag {
			return true
		}
	}
	return false
}

// NewComponentList returns only the components that should be included in list of available new components.
func (v Components) NewComponentList() (Components, error) {
	// Filter out:
	//	- deprecated
	//  - not published
	//  - processors
	//  - code patterns
	components := make(Components, 0)
	for _, c := range v {
		if !c.IsDeprecated() && !c.IsExcludedFromNewList() && !c.IsCodePattern() && !c.IsProcessor() {
			components = append(components, c)
		}
	}

	// Sort "keboola" vendor first
	sort.SliceStable(components, func(i, j int) bool {
		idI := components[i].ID
		idJ := components[j].ID

		// Components from keboola vendor will be first
		vendor := `keboola.`
		vendorI := strings.HasPrefix(string(idI), vendor)
		vendorJ := strings.HasPrefix(string(idJ), vendor)
		if vendorI != vendorJ {
			return vendorI
		}

		// Sort by ID otherwise
		return idI < idJ
	})

	return components, nil
}
