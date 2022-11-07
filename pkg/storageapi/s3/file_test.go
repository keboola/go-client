package s3_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	fileTest "github.com/keboola/go-client/pkg/storageapi/file"
)

func TestFileApiCreateFileResource(t *testing.T) {
	t.Parallel()
	c := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageS3())

	for _, tc := range fileTest.UploadTestCases() {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			runUploadTest(t, c, tc.File)
		})
	}
}

func runUploadTest(t *testing.T, c client.Sender, f *storageapi.File) {
	t.Helper()
	ctx := context.Background()

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

	// Upload
	reader := strings.NewReader("col1,col2\nval1,val2\n")
	written, err := storageapi.Upload(ctx, file, reader)
	assert.NotEmpty(t, written)
	assert.NoError(t, err)

	// Get file resource
	file, err = storageapi.GetFileResourceRequest(file.ID).Send(ctx, c)
	assert.NoError(t, err)

	// Check uploaded file
	res, err := fileTest.GetUploadedFile(t, file)
	assert.NoError(t, err)
	assert.Equal(t, "col1,col2\nval1,val2\n", res)
}
