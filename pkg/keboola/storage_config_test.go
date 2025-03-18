package keboola_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/keboola"
)

func TestConfigApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, components)

	// Create config with rows
	row1 := &ConfigRow{
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        false,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row1", Value: "value1"},
		}),
	}
	row2 := &ConfigRow{
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        true,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row2", Value: "value2"},
		}),
	}
	config := &ConfigWithRows{
		Config: &Config{
			ConfigKey: ConfigKey{
				BranchID:    branch.ID,
				ComponentID: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
			RowsSortOrder: []string{},
		},
		Rows: []*ConfigRow{row1, row2},
	}
	resConfig, err := api.CreateConfigRequest(config).Send(ctx)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.ID)
	assert.Equal(t, config.ID, row1.ConfigID)
	assert.Equal(t, ComponentID("ex-generic-v2"), row1.ComponentID)
	assert.Equal(t, branch.ID, row1.BranchID)
	assert.Equal(t, config.ID, row2.ConfigID)
	assert.Equal(t, ComponentID("ex-generic-v2"), row2.ComponentID)
	assert.Equal(t, branch.ID, row2.BranchID)

	// Get config
	resultConfig, err := api.GetConfigRequest(config.ConfigKey).Send(ctx)
	assert.NoError(t, err)

	// Change description and version differs, because rows have been created after the configuration has been created.
	config.ChangeDescription = resultConfig.ChangeDescription
	config.Version = resultConfig.Version
	assert.Equal(t, config.Config, resultConfig)

	// List configs (should contain 1)
	configList, err := api.ListConfigRequest(config.BranchID, config.ComponentID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *configList, 1)
	assert.Equal(t, config.Config, (*configList)[0])

	// Create a new row (row3) and add it to the existing configuration
	row3 := &ConfigRow{
		Name:              "Row3",
		Description:       "Row3 description",
		ChangeDescription: "Row3 test",
		IsDisabled:        false,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row3", Value: "value3"},
		}),
		ConfigRowKey: ConfigRowKey{
			BranchID:    config.BranchID,
			ComponentID: config.ComponentID,
			ConfigID:    config.ID,
		},
	}

	config.Rows = append(config.Rows, row3)

	// Update config
	config.Name = "Test modified +++úěš!@#"
	config.Description = "Test description modified"
	config.ChangeDescription = "updated"
	config.Content = orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "foo",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "bar", Value: "modified"},
			}),
		},
	})
	resConfig, err = api.UpdateConfigRequest(config, []string{"name", "description", "changeDescription", "configuration"}).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, *config.Config, *resConfig.Config)

	// List components
	components, err = api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NotEmpty(t, components)
	assert.NoError(t, err)
	componentsJSON, err := json.MarshalIndent(components, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedComponentsConfigTest(), string(componentsJSON), "Unexpected components")

	// Update metadata
	metadata := map[string]string{"KBC.KaC.meta1": "value"}
	_, err = api.AppendConfigMetadataRequest(config.ConfigKey, metadata).Send(ctx)
	assert.NoError(t, err)

	// List metadata
	configsMetadata, err := api.ListConfigMetadataRequest(branch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[ConfigKey]Metadata{
		config.ConfigKey: map[string]string{"KBC.KaC.meta1": "value"},
	}, configsMetadata.ToMap())

	// Delete metadata
	_, err = api.DeleteConfigMetadataRequest(config.ConfigKey, (*configsMetadata)[0].Metadata[0].ID).Send(ctx)
	assert.NoError(t, err)

	// Check that metadata is deleted
	configsMetadata, err = api.ListConfigMetadataRequest(branch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, configsMetadata)

	// Delete configuration
	_, err = api.DeleteConfigRequest(config.ConfigKey).Send(ctx)
	assert.NoError(t, err)

	// List components - no component
	components, err = api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, components)
}

func expectedComponentsConfigTest() string {
	return `[
  {
    "branchId": %s,
    "id": "ex-generic-v2",
    "type": "extractor",
    "name": "Generic",
    %A,
    "configurations": [
      {
        "branchId": %s,
        "componentId": "ex-generic-v2",
        "id": "%s",
        "name": "Test modified +++úěš!@#",
        "description": "Test description modified",
        "changeDescription": "Row%d test",
        "isDeleted": false,
        "created": "%s",
        "version": 7,
        "state": null,
        "isDisabled": false,
        "configuration": {
          "foo": {
            "bar": "modified"
          }
        },
        "rows": [
          {
            "id": "%s",
            "name": "Row1",
            "description": "Row1 description",
            "changeDescription": "Row1 test",
            "isDisabled": false,
            "version": 2,
            "state": null,
            "configuration": {
              "row1": "value1"
            }
          },
          {
            "id": "%s",
            "name": "Row2",
            "description": "Row2 description",
            "changeDescription": "Row2 test",
            "isDisabled": true,
            "version": 2,
            "state": null,
            "configuration": {
              "row2": "value2"
            }
          },
          {
            "id": "%s",
            "name": "Row3",
            "description": "Row3 description",
            "changeDescription": "Row3 test",
            "isDisabled": false,
            "version": 1,
            "state": null,
            "configuration": {
              "row3": "value3"
            }
          }
        ]
      }
    ]
  }
]
`
}
