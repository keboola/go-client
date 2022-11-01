package storageapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestFileApiCreateFileResourceS3(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := clientForAnEmptyProject(t, testproject.WithStagingStorage("s3"))

	// Create file
	f := &File{
		IsPublic:    false,
		IsSliced:    true,
		IsEncrypted: true,
		Name:        "test",
		Tags:        []string{"tag1", "tag2"},
		ContentType: "text/csv",
	}

	file, err := CreateFileResourceRequest(f).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, file.ID)
	assert.NotEmpty(t, file.Url)
	assert.NotEmpty(t, file.Created)
	assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)
	assert.NotEmpty(t, file.UploadParams)
	assert.NotEmpty(t, file.UploadParams.Bucket)
	assert.NotEmpty(t, file.UploadParams.Credentials.AccessKeyId)
	assert.NotEmpty(t, file.UploadParams.Credentials.SecretAccessKey)
}

func TestFileApiCreateFileResourceABS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := clientForAnEmptyProject(t, testproject.WithStagingStorage("abs"))

	// Create file
	f := &File{
		IsPublic:    false,
		IsSliced:    true,
		IsEncrypted: true,
		Name:        "test",
		Tags:        []string{"tag1", "tag2"},
		ContentType: "text/csv",
	}

	file, err := CreateFileResourceRequest(f).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, file.ID)
	assert.NotEmpty(t, file.Url)
	assert.NotEmpty(t, file.Created)
	assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)
	assert.NotEmpty(t, file.AbsUploadParams)
	assert.NotEmpty(t, file.AbsUploadParams.BlobName)
	assert.NotEmpty(t, file.AbsUploadParams.Credentials.SASConnectionString)
}
