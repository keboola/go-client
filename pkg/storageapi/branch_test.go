package storageapi_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
)

func TestBranchApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForAnEmptyProject(t)

	// Get default branch
	defaultBranch, err := GetDefaultBranchRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotNil(t, defaultBranch)
	assert.Equal(t, "Main", defaultBranch.Name)
	assert.True(t, defaultBranch.IsDefault)

	// Default branch cannot be created
	assert.PanicsWithError(t, "default branch cannot be created", func() {
		CreateBranchRequest(&Branch{
			Name:        "Foo",
			Description: "Foo branch",
			IsDefault:   true,
		}).Send(ctx, c)
	})

	// Create branch, wait for successful job status
	branchFoo := &Branch{
		Name:        "Foo",
		Description: "Foo branch",
		IsDefault:   false,
	}
	_, err = CreateBranchRequest(branchFoo).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, branchFoo.ID)

	// Get branch
	resultBranch, err := GetBranchRequest(branchFoo.BranchKey).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, branchFoo, resultBranch)

	// Create branch, already exists
	branchFooDuplicate := &Branch{
		Name:        "Foo",
		Description: "Foo branch 2",
		IsDefault:   false,
	}
	_, err = CreateBranchRequest(branchFooDuplicate).Send(ctx, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "There already is a branch with name \"Foo\"")

	// Update branch
	branchFoo.Name = "Foo modified"
	branchFoo.Description = "Foo description modified"
	_, err = UpdateBranchRequest(branchFoo, []string{"name", "description"}).Send(ctx, c)
	assert.NoError(t, err)

	// Update main branch description
	defaultBranch.Description = "Default branch"
	_, err = UpdateBranchRequest(defaultBranch, []string{"description"}).Send(ctx, c)
	assert.NoError(t, err)

	// Can update default branch name
	defaultBranch.Name = "Not Allowed"
	assert.PanicsWithError(t, `the name of the main branch cannot be changed`, func() {
		UpdateBranchRequest(defaultBranch, []string{"name", "description"}).Send(ctx, c)
	})

	// List branches
	branches, err := ListBranchesRequest().Send(ctx, c)
	assert.NotNil(t, branches)
	assert.NoError(t, err)
	branchesJson, err := json.MarshalIndent(branches, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedBranchesAll(), string(branchesJson), "Unexpected branches state")

	// Append branch metadata
	_, err = AppendBranchMetadataRequest(branchFoo.BranchKey, map[string]string{"KBC.KaC.meta1": "value", "KBC.KaC.meta2": "value"}).Send(ctx, c)
	assert.NoError(t, err)

	// List metadata
	metadata, err := ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, Metadata{"KBC.KaC.meta1": "value", "KBC.KaC.meta2": "value"}, metadata.ToMap())

	// Append metadata with empty value
	_, err = AppendBranchMetadataRequest(branchFoo.BranchKey, map[string]string{"KBC.KaC.meta2": ""}).Send(ctx, c)
	assert.NoError(t, err)

	// Check that metadata is deleted
	metadata, err = ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, Metadata{"KBC.KaC.meta1": "value"}, metadata.ToMap())

	// Delete metadata
	_, err = DeleteBranchMetadataRequest(branchFoo.BranchKey, (*metadata)[0].ID).Send(ctx, c)
	assert.NoError(t, err)

	// Check that metadata is deleted
	metadata, err = ListBranchMetadataRequest(branchFoo.BranchKey).Send(ctx, c)
	assert.NoError(t, err)
	assert.Empty(t, metadata)

	// Delete branch
	_, err = DeleteBranchRequest(branchFoo.BranchKey).Send(ctx, c)
	assert.NoError(t, err)

	// Check that branch has been deleted
	branches, err = ListBranchesRequest().Send(ctx, c)
	assert.NotNil(t, branches)
	assert.NoError(t, err)
	branchesJson, err = json.MarshalIndent(branches, "", "  ")
	assert.NoError(t, err)
	wildcards.Assert(t, expectedBranchesMain(), string(branchesJson), "Unexpected branches state")
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
