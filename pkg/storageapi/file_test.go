package storageapi

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListAndDeleteFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := ClientForAnEmptyProject(t)

	// Create two files
	file1 := &File{Name: "test1", IsEncrypted: true, FederationToken: true}
	_, err := CreateFileResourceRequest(file1).Send(ctx, client)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	file2 := &File{Name: "test2", IsEncrypted: true, FederationToken: true}
	_, err = CreateFileResourceRequest(file2).Send(ctx, client)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err := ListFilesRequest().Send(ctx, client)
	assert.NoError(t, err)
	assert.Len(t, *files, 2)
	assert.Equal(t, file1.ID, (*files)[0].ID)
	assert.Equal(t, file2.ID, (*files)[1].ID)

	// Delete file1
	_, err = DeleteFileRequest(file1.ID).Send(ctx, client)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = ListFilesRequest().Send(ctx, client)
	assert.NoError(t, err)
	assert.Len(t, *files, 1)
	assert.Equal(t, file2.ID, (*files)[0].ID)

	// Delete file2
	_, err = DeleteFileRequest(file2.ID).Send(ctx, client)
	assert.NoError(t, err)

	// List
	time.Sleep(1 * time.Second)
	files, err = ListFilesRequest().Send(ctx, client)
	assert.NoError(t, err)
	assert.Empty(t, files)
}
