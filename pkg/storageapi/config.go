package storageapi

import (
	"context"
	"fmt"
	"sort"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

// ConfigID represents an ID of a configuration in Storage API.
type ConfigID string

func (v ConfigID) String() string {
	return string(v)
}

// ConfigKey is a unique identifier of a configuration.
type ConfigKey struct {
	BranchID    BranchID    `json:"branchId"`
	ComponentID ComponentID `json:"componentId"`
	ID          ConfigID    `json:"id" writeas:"configurationId" writeoptional:"true"`
}

func (k ConfigKey) ObjectId() any {
	return k.ID
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDeleted         bool                   `json:"isDeleted" readonly:"true"`
	Created           iso8601.Time           `json:"created" readonly:"true"`
	Version           int                    `json:"version" readonly:"true"`
	State             *orderedmap.OrderedMap `json:"state" readonly:"true"`
	IsDisabled        bool                   `json:"isDisabled"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
}

// ConfigWithRows is a configuration with its configuration rows.
type ConfigWithRows struct {
	*Config
	Rows []*ConfigRow `json:"rows"`
}

// SortRows by name.
func (c *ConfigWithRows) SortRows() {
	sort.SliceStable(c.Rows, func(i, j int) bool {
		return c.Rows[i].Name < c.Rows[j].Name
	})
}

// ConfigMetadataItem is one item of configuration metadata.
type ConfigMetadataItem struct {
	BranchID    BranchID
	ComponentID ComponentID     `json:"idComponent"`
	ConfigID    ConfigID        `json:"configurationId"`
	Metadata    MetadataDetails `json:"metadata"`
}

// ConfigsMetadata slice.
type ConfigsMetadata []*ConfigMetadataItem

// ToMap converts slice to map.
func (v ConfigsMetadata) ToMap() map[ConfigKey]Metadata {
	out := make(map[ConfigKey]Metadata)
	for _, item := range v {
		key := ConfigKey{BranchID: item.BranchID, ComponentID: item.ComponentID, ID: item.ConfigID}
		out[key] = item.Metadata.ToMap()
	}
	return out
}

// ListConfigsAndRowsFrom https://keboola.docs.apiary.io/#reference/components-and-configurations/get-components/get-components
func ListConfigsAndRowsFrom(branch BranchKey) client.APIRequest[*[]*ComponentWithConfigs] {
	result := make([]*ComponentWithConfigs, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("branch/{branchId}/components").
		AndPathParam("branchId", branch.ID.String()).
		AndQueryParam("include", "configuration,rows").
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.HTTPResponse) error {
			// Add missing values
			for _, component := range result {
				component.BranchID = branch.ID

				// Set config IDs
				for _, config := range component.Configs {
					config.BranchID = branch.ID
					config.ComponentID = component.ID
					config.SortRows()

					// Set rows IDs
					for _, row := range config.Rows {
						row.BranchID = branch.ID
						row.ComponentID = component.ID
						row.ConfigID = config.ID
					}
				}
			}
			return nil
		})
	return client.NewAPIRequest(&result, request)
}

func ListConfigRequest(branchId BranchID, componentId ComponentID) client.APIRequest[*[]*Config] {
	result := make([]*Config, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("branch/{branchId}/components/{componentId}/configs").
		AndPathParam("branchId", branchId.String()).
		AndPathParam("componentId", componentId.String()).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, response client.HTTPResponse) error {
			for _, c := range result {
				c.BranchID = branchId
				c.ComponentID = componentId
			}
			return nil
		})
	return client.NewAPIRequest(&result, request)
}

// GetConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/development-branch-configuration-detail
func GetConfigRequest(key ConfigKey) client.APIRequest[*Config] {
	result := &Config{}
	result.BranchID = key.BranchID
	result.ComponentID = key.ComponentID
	request := newRequest().
		WithResult(result).
		WithGet("branch/{branchId}/components/{componentId}/configs/{configId}").
		AndPathParam("branchId", key.BranchID.String()).
		AndPathParam("componentId", key.ComponentID.String()).
		AndPathParam("configId", key.ID.String())
	return client.NewAPIRequest(result, request)
}

// CreateConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/create-development-branch-configuration
func CreateConfigRequest(config *ConfigWithRows) client.APIRequest[*ConfigWithRows] {
	// Create config
	request := newRequest().
		WithResult(config).
		WithPost("branch/{branchId}/components/{componentId}/configs").
		AndPathParam("branchId", config.BranchID.String()).
		AndPathParam("componentId", string(config.ComponentID)).
		WithFormBody(client.ToFormBody(client.StructToMap(config.Config, nil))).
		WithOnError(ignoreResourceAlreadyExistsError(func(ctx context.Context, sender client.Sender) error {
			if result, err := GetConfigRequest(config.ConfigKey).Send(ctx, sender); err == nil {
				*config.Config = *result
				return nil
			} else {
				return err
			}
		})).
		// Create config rows
		WithOnSuccess(func(ctx context.Context, sender client.Sender, _ client.HTTPResponse) error {
			for _, row := range config.Rows {
				row := row
				row.BranchID = config.BranchID
				row.ComponentID = config.ComponentID
				row.ConfigID = config.ID
				if _, err := CreateConfigRowRequest(row).Send(ctx, sender); err != nil {
					return err
				}
			}
			return nil
		})
	return client.NewAPIRequest(config, request)
}

// UpdateConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/update-development-branch-configuration
func UpdateConfigRequest(config *Config, changedFields []string) client.APIRequest[*Config] {
	// ID is required
	if config.ID == "" {
		panic("config id must be set")
	}

	// Update config
	request := newRequest().
		WithResult(config).
		WithPut("branch/{branchId}/components/{componentId}/configs/{configId}").
		AndPathParam("branchId", config.BranchID.String()).
		AndPathParam("componentId", string(config.ComponentID)).
		AndPathParam("configId", string(config.ID)).
		WithFormBody(client.ToFormBody(client.StructToMap(config, changedFields)))
	return client.NewAPIRequest(config, request)
}

// DeleteConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/delete-configuration
func DeleteConfigRequest(config ConfigKey) client.APIRequest[client.NoResult] {
	request := newRequest().
		WithDelete("branch/{branchId}/components/{componentId}/configs/{configId}").
		AndPathParam("branchId", config.BranchID.String()).
		AndPathParam("componentId", string(config.ComponentID)).
		AndPathParam("configId", string(config.ID)).
		WithOnError(ignoreResourceNotFoundError())
	return client.NewAPIRequest(client.NoResult{}, request)
}

// DeleteConfigsInBranchRequest lists all configs in branch and deletes them all.
func DeleteConfigsInBranchRequest(branch BranchKey) client.APIRequest[client.NoResult] {
	request := ListConfigsAndRowsFrom(branch).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*ComponentWithConfigs) error {
			wg := client.NewWaitGroup(ctx, sender)
			for _, component := range *result {
				for _, config := range component.Configs {
					config := config
					wg.Send(DeleteConfigRequest(config.ConfigKey))
				}
			}
			return wg.Wait()
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}

// ListConfigMetadataRequest https://keboola.docs.apiary.io/#reference/search/search-components-configurations/search-component-configurations
func ListConfigMetadataRequest(branchID BranchID) client.APIRequest[*ConfigsMetadata] {
	result := make(ConfigsMetadata, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("branch/{branchId}/search/component-configurations").
		AndPathParam("branchId", branchID.String()).
		AndQueryParam("include", "filteredMetadata").
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.HTTPResponse) error {
			for _, item := range result {
				item.BranchID = branchID
			}
			return nil
		})
	return client.NewAPIRequest(&result, request)
}

// AppendConfigMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/components-configurations-metadata/create-or-update
func AppendConfigMetadataRequest(key ConfigKey, metadata Metadata) client.APIRequest[client.NoResult] {
	// Empty, we have nothing to append
	if len(metadata) == 0 {
		return client.NewNoOperationAPIRequest(client.NoResult{})
	}
	formBody := make(map[string]string)
	i := 0
	for k, v := range metadata {
		formBody[fmt.Sprintf("metadata[%d][key]", i)] = k
		formBody[fmt.Sprintf("metadata[%d][value]", i)] = v
		i++
	}
	request := newRequest().
		WithPost("branch/{branchId}/components/{componentId}/configs/{configId}/metadata").
		AndPathParam("branchId", key.BranchID.String()).
		AndPathParam("componentId", string(key.ComponentID)).
		AndPathParam("configId", string(key.ID)).
		WithFormBody(formBody)
	return client.NewAPIRequest(client.NoResult{}, request)
}

// DeleteConfigMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/components-configurations-metadata/delete
func DeleteConfigMetadataRequest(key ConfigKey, metaID string) client.APIRequest[client.NoResult] {
	request := newRequest().
		WithDelete("branch/{branchId}/components/{componentId}/configs/{configId}/metadata/{metadataId}").
		AndPathParam("branchId", key.BranchID.String()).
		AndPathParam("componentId", string(key.ComponentID)).
		AndPathParam("configId", string(key.ID)).
		AndPathParam("metadataId", metaID).
		WithOnError(ignoreResourceNotFoundError())
	return client.NewAPIRequest(client.NoResult{}, request)
}
