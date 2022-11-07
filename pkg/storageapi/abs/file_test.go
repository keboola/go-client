package abs_test

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
	"github.com/keboola/go-client/pkg/storageapi/abs"
)

func TestFileApiCreateFileResource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageABS())

	// Create file
	f := &storageapi.File{
		IsPublic:    false,
		IsSliced:    true,
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
	assert.NotEmpty(t, file.ABSUploadParams)
	assert.NotEmpty(t, file.ABSUploadParams.BlobName)
	assert.NotEmpty(t, file.ABSUploadParams.Credentials.SASConnectionString)

	// Connect ABS bucket
	writer, err := abs.NewWriter(ctx, file.ABSUploadParams)
	assert.NoError(t, err)

	// Upload
	reader := strings.NewReader("col1,col2\nval1,val2\n")
	written, err := storageapi.Upload(writer, reader)
	assert.NotEmpty(t, written)
	assert.NoError(t, err)

	// Get file resource
	file, err = storageapi.GetFileResourceRequest(file.ID).Send(ctx, c)
	assert.NoError(t, err)

	// Check uploaded file
	resp, err := http.Get(file.Url) //nolint:noctx
	assert.NoError(t, err)
	defer resp.Body.Close()
	gr, err := gzip.NewReader(resp.Body)
	assert.NoError(t, err)
	o, err := io.ReadAll(gr)
	assert.NoError(t, err)
	assert.Equal(t, "col1,col2\nval1,val2\n", string(o))
}
