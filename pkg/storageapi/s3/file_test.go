package s3_test

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/s3"
)

func TestFileApiCreateFileResource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageS3())

	// Create file
	f := &storageapi.File{
		IsPublic:    false,
		IsSliced:    false,
		IsEncrypted: true,
		Name:        "test",
		Tags:        []string{"tag1", "tag2"},
		ContentType: "text/csv",
	}

	file, err := storageapi.CreateFileResourceRequest(f).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, file.ID)
	assert.NotEmpty(t, file.Url)
	assert.NotEmpty(t, file.Created)
	assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)
	assert.NotEmpty(t, file.S3UploadParams)
	assert.NotEmpty(t, file.S3UploadParams.Bucket)
	assert.NotEmpty(t, file.S3UploadParams.Credentials.AccessKeyId)
	assert.NotEmpty(t, file.S3UploadParams.Credentials.SecretAccessKey)

	// Connect S3 bucket
	writer, err := s3.CreateBucketWriter(ctx, file.S3UploadParams, file.Region, file.IsEncrypted)
	assert.NoError(t, err)

	// Upload
	reader := io.NopCloser(strings.NewReader("col1,col2\nval1,val2\n"))
	err = storageapi.Upload(writer, reader)
	assert.NoError(t, err)

	// Get file resource
	file, err = storageapi.GetFileResourceRequest(file.ID).Send(ctx, c)
	assert.NoError(t, err)

	// Check uploaded file
	resp, err := http.Get(file.Url) //nolint:noctx
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.NoError(t, err)
	gr, err := gzip.NewReader(resp.Body)
	assert.NoError(t, err)
	o, err := io.ReadAll(gr)
	assert.NoError(t, err)
	assert.Equal(t, "col1,col2\nval1,val2\n", string(o))
}
