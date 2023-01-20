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

func TestConfigRowApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	branch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// Create config
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
	}
	_, err = api.CreateConfigRequest(config).Send(ctx)
	assert.NoError(t, err)

	// Create row1
	row1 := &ConfigRow{
		ConfigRowKey: ConfigRowKey{
			BranchID:    branch.ID,
			ComponentID: "ex-generic-v2",
			ConfigID:    config.ID,
		},
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        true,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row1", Value: "value1"},
		}),
	}
	_, err = api.CreateConfigRowRequest(row1).Send(ctx)
	assert.NoError(t, err)

	// Get row1
	resultRow, err := api.GetConfigRowRequest(row1.ConfigRowKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, row1, resultRow)

	// Create row2
	row2 := &ConfigRow{
		ConfigRowKey: ConfigRowKey{
			BranchID:    branch.ID,
			ComponentID: "ex-generic-v2",
			ConfigID:    config.ID,
		},
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        false,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row2", Value: "value2"},
		}),
	}
	_, err = api.CreateConfigRowRequest(row2).Send(ctx)
	assert.NoError(t, err)

	// Update row 1
	row1.Name = "Row1 modified"
	row1.Description = "Row1 description modified"
	row1.ChangeDescription = "updated"
	row1.Content = orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "row1", Value: "xyz"},
	})
	_, err = api.UpdateConfigRowRequest(row1, []string{"name", "description", "changeDescription", "configuration"}).Send(ctx)
	assert.NoError(t, err)

	// Delete row 2
	_, err = api.DeleteConfigRowRequest(row2.ConfigRowKey).Send(ctx)
	assert.NoError(t, err)

	// List components
	components, err := api.ListConfigsAndRowsFrom(branch.BranchKey).Send(ctx)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	componentsJSON, err := json.MarshalIndent(components, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedComponentsConfigRowTest(), string(componentsJSON), "Unexpected components")
}

func expectedComponentsConfigRowTest() string {
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
        "name": "Test",
        "description": "Test description",
        "changeDescription": "Row %s deleted",
        "isDeleted": false,
        "created": "%s",
        "version": 5,
        "state": null,
        "isDisabled": false,
        "configuration": {
          "foo": {
            "bar": "baz"
          }
        },
        "rows": [
          {
            "id": "%s",
            "name": "Row1 modified",
            "description": "Row1 description modified",
            "changeDescription": "updated",
            "isDisabled": true,
            "version": 2,
            "state": null,
            "configuration": {
              "row1": "xyz"
            }
          }
        ]
      }
    ]
  }
]
`
}
