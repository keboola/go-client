package storageapi_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestConfigApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForAnEmptyProject(t)

	// Get default branch
	branch, err := GetDefaultBranchRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List - no component/config
	components, err := ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, c)
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
		},
		Rows: []*ConfigRow{row1, row2},
	}
	resConfig, err := CreateConfigRequest(config).Send(ctx, c)
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
	resultConfig, err := GetConfigRequest(config.ConfigKey).Send(ctx, c)
	assert.NoError(t, err)

	// Change description and version differs, because rows have been created after the configuration has been created.
	config.ChangeDescription = resultConfig.ChangeDescription
	config.Version = resultConfig.Version
	assert.Equal(t, config.Config, resultConfig)

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
	_, err = UpdateConfigRequest(config.Config, []string{"name", "description", "changeDescription", "configuration"}).Send(ctx, c)
	assert.NoError(t, err)

	// List components
	components, err = ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, c)
	assert.NotEmpty(t, components)
	assert.NoError(t, err)
	componentsJson, err := json.MarshalIndent(components, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedComponentsConfigTest(), string(componentsJson), "Unexpected components")

	// Update metadata
	metadata := map[string]string{"KBC.KaC.meta1": "value"}
	_, err = AppendConfigMetadataRequest(config.ConfigKey, metadata).Send(ctx, c)
	assert.NoError(t, err)

	// List metadata
	configsMetadata, err := ListConfigMetadataRequest(branch.ID).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, map[ConfigKey]Metadata{
		config.ConfigKey: map[string]string{"KBC.KaC.meta1": "value"},
	}, configsMetadata.ToMap())

	// Delete metadata
	_, err = DeleteConfigMetadataRequest(config.ConfigKey, (*configsMetadata)[0].Metadata[0].ID).Send(ctx, c)
	assert.NoError(t, err)

	// Check that metadata is deleted
	configsMetadata, err = ListConfigMetadataRequest(branch.ID).Send(ctx, c)
	assert.NoError(t, err)
	assert.Empty(t, configsMetadata)

	// Delete configuration
	_, err = DeleteConfigRequest(config.ConfigKey).Send(ctx, c)
	assert.NoError(t, err)

	// List components - no component
	components, err = ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx, c)
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
    "flags": [
      "genericUI",
      "encrypt"
    ],
    "configurationSchema": {},
    "configurationRowSchema": {},
    "emptyConfiguration": {},
    "emptyConfigurationRow": {},
    "data": {
      "default_bucket": false,
      "default_bucket_stage": ""
    },
    "configurations": [
      {
        "branchId": %s,
        "componentId": "ex-generic-v2",
        "id": "%s",
        "name": "Test modified +++úěš!@#",
        "description": "Test description modified",
        "changeDescription": "updated",
        "isDeleted": false,
        "created": "%s",
        "version": 4,
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
            "version": 1,
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
            "version": 1,
            "state": null,
            "configuration": {
              "row2": "value2"
            }
          }
        ]
      }
    ]
  }
]
`
}
