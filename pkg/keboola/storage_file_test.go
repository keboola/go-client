package keboola

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx, testproject.WithStagingStorageABS())

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	// Create two files
	file1, err := api.CreateFileResourceRequest(defBranch.ID, "test1").Send(ctx)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	file2, err := api.CreateFileResourceRequest(defBranch.ID, "test2").Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err := api.ListFilesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *files, 2)
	assert.Equal(t, file1.FileID, (*files)[0].FileID)
	assert.Equal(t, file2.FileID, (*files)[1].FileID)

	// Get
	resp1, err := api.GetFileRequest(file1.FileKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, resp1.FileID, file1.FileID)

	// Get with download credentials
	resp2, err := api.GetFileWithCredentialsRequest(file1.FileKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, resp2.FileID, file1.FileID)
	assert.True(t,
		(resp2.S3DownloadParams != nil && resp2.S3DownloadParams.Path.Key != "") ||
			(resp2.ABSDownloadParams != nil && resp2.ABSDownloadParams.Path.BlobName != "") ||
			(resp2.GCSDownloadParams != nil && resp2.GCSDownloadParams.Path.Key != ""),
	)

	// Delete file1
	_, err = api.DeleteFileRequest(file1.FileKey).Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = api.ListFilesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *files, 1)
	assert.Equal(t, file2.FileID, (*files)[0].FileID)

	// Delete file2
	_, err = api.DeleteFileRequest(file2.FileKey).Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = api.ListFilesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, files)
}
