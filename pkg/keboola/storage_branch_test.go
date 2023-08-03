package keboola_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/keboola"
)

func TestBranchApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defaultBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, defaultBranch)
	assert.Equal(t, "Main", defaultBranch.Name)
	assert.True(t, defaultBranch.IsDefault)

	// Default branch cannot be created
	assert.PanicsWithError(t, "default branch cannot be created", func() {
		api.CreateBranchRequest(&Branch{
			Name:        "Foo",
			Description: "Foo branch",
			IsDefault:   true,
		}).Send(ctx)
	})

	// Create branch, wait for successful job status
	branchFoo := &Branch{
		Name:        "Foo",
		Description: "Foo branch",
		IsDefault:   false,
	}
	_, err = api.CreateBranchRequest(branchFoo).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, branchFoo.ID)

	// Get branch
	resultBranch, err := api.GetBranchRequest(branchFoo.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, branchFoo, resultBranch)

	// Create branch, already exists
	branchFooDuplicate := &Branch{
		Name:        "Foo",
		Description: "Foo branch 2",
		IsDefault:   false,
	}
	_, err = api.CreateBranchRequest(branchFooDuplicate).Send(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "There already is a branch with name \"Foo\"")

	// Update branch
	branchFoo.Name = "Foo modified"
	branchFoo.Description = "Foo description modified"
	_, err = api.UpdateBranchRequest(branchFoo, []string{"name", "description"}).Send(ctx)
	assert.NoError(t, err)

	// Update main branch description
	defaultBranch.Description = "Default branch"
	_, err = api.UpdateBranchRequest(defaultBranch, []string{"description"}).Send(ctx)
	assert.NoError(t, err)

	// Can update default branch name
	defaultBranch.Name = "Not Allowed"
	assert.PanicsWithError(t, `the name of the main branch cannot be changed`, func() {
		api.UpdateBranchRequest(defaultBranch, []string{"name", "description"}).Send(ctx)
	})

	// List branches
	branches, err := api.ListBranchesRequest().Send(ctx)
	assert.NotNil(t, branches)
	assert.NoError(t, err)
	branchesJSON, err := json.MarshalIndent(branches, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedBranchesAll(), string(branchesJSON), "Unexpected branches state")

	// Append branch metadata
	_, err = api.AppendBranchMetadataRequest(branchFoo.BranchKey, map[string]string{"KBC.KaC.meta1": "value", "KBC.KaC.meta2": "value"}).Send(ctx)
	assert.NoError(t, err)

	// List metadata
	metadata, err := api.ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, Metadata{"KBC.KaC.meta1": "value", "KBC.KaC.meta2": "value"}, metadata.ToMap())

	// Append metadata with empty value
	_, err = api.AppendBranchMetadataRequest(branchFoo.BranchKey, map[string]string{"KBC.KaC.meta2": ""}).Send(ctx)
	assert.NoError(t, err)

	// Check that metadata is deleted
	metadata, err = api.ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, Metadata{"KBC.KaC.meta1": "value"}, metadata.ToMap())

	// Delete metadata
	_, err = api.DeleteBranchMetadataRequest(branchFoo.BranchKey, (*metadata)[0].ID).Send(ctx)
	assert.NoError(t, err)

	// Check that metadata is deleted
	metadata, err = api.ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, metadata)

	// Delete branch
	_, err = api.DeleteBranchRequest(branchFoo.BranchKey).Send(ctx)
	assert.NoError(t, err)

	// Check that branch has been deleted
	branches, err = api.ListBranchesRequest().Send(ctx)
	assert.NotNil(t, branches)
	assert.NoError(t, err)
	branchesJSON, err = json.MarshalIndent(branches, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedBranchesMain(), string(branchesJSON), "Unexpected branches state")
}

func expectedBranchesAll() string {
	return `[
  {
    "id": %s,
    "name": "Foo modified",
    "description": "Foo description modified",
    "created": "%s",
    "isDefault": false
  },
  {
    "id": %s,
    "name": "Main",
    "description": "Default branch",
    "created": "%s",
    "isDefault": true
  }
]`
}

func expectedBranchesMain() string {
	return `[
  {
    "id": %s,
    "name": "Main",
    "description": "Default branch",
    "created": "%s",
    "isDefault": true
  }
]`
}
