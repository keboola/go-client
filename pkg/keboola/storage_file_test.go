package keboola

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestFileOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx, testproject.WithStagingStorageABS())

	// Create two files
	file1, err := api.CreateFileResourceRequest("test1").Send(ctx)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	file2, err := api.CreateFileResourceRequest("test2").Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err := api.ListFilesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *files, 2)
	assert.Equal(t, file1.ID, (*files)[0].ID)
	assert.Equal(t, file2.ID, (*files)[1].ID)

	// Get
	resp1, err := api.GetFileRequest(file1.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, resp1.ID, file1.ID)

	// Get with download credentials
	resp2, err := api.GetFileWithCredentialsRequest(file1.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, resp2.ID, file1.ID)
	assert.True(t,
		(resp2.S3DownloadParams != nil && resp2.S3DownloadParams.Path.Key != "") ||
			(resp2.ABSDownloadParams != nil && resp2.ABSDownloadParams.Path.BlobName != "") ||
			(resp2.GCSDownloadParams != nil && resp2.GCSDownloadParams.Path.Key != ""),
	)

	// Delete file1
	_, err = api.DeleteFileRequest(file1.ID).Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = api.ListFilesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *files, 1)
	assert.Equal(t, file2.ID, (*files)[0].ID)

	// Delete file2
	_, err = api.DeleteFileRequest(file2.ID).Send(ctx)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = api.ListFilesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, files)
}
