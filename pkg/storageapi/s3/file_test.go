package s3_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestFileApiCreateFileResource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := ClientForAnEmptyProject(t, testproject.WithStagingStorageS3())

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
	assert.NotEmpty(t, file.S3UploadParams)
	assert.NotEmpty(t, file.S3UploadParams.Bucket)
	assert.NotEmpty(t, file.S3UploadParams.Credentials.AccessKeyId)
	assert.NotEmpty(t, file.S3UploadParams.Credentials.SecretAccessKey)
}
