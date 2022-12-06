package gcs_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/gcs"
	"github.com/keboola/go-client/pkg/storageapi/testdata"
)

func TestCreateFileResourceAndUpload(t *testing.T) {
	t.Skip("Waiting for production GCP stack")
	t.Parallel()
	storageApiClient := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageGCS())
	for _, tc := range testdata.UploadTestCases() {
		tc.Run(t, storageApiClient)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()

	f := &storageapi.File{
		Provider: "gcp",
		GCSUploadParams: &gcs.UploadParams{
			Key:    "exp-15-files-4516-27298008-2022-11-08.test1",
			Bucket: "kbc-sapi-files",
		},
	}

	res, err := storageapi.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &storageapi.SlicedFileManifest{Entries: []storageapi.Slice{
		{Url: "gs://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1one"},
		{Url: "gs://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1two"},
	}}
	assert.Equal(t, e, res)
}

func TestTransportRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("POST", `https://storage.googleapis.com/upload/storage/v1/b/bucket/o`, httpmock.NewStringResponder(504, "test"))

	params := &gcs.UploadParams{
		ProjectId:   "project",
		Key:         "key",
		Bucket:      "bucket",
		AccessToken: "token",
		TokenType:   "Bearer",
		ExpiresIn:   0,
	}
	bw, err := gcs.NewUploadWriter(context.Background(), params, "", transport)
	assert.NoError(t, err)
	content := []byte("col1,col2\nval1,val2\n")
	_, err = bw.Write(content)
	assert.NoError(t, err)
	assert.ErrorContains(t, bw.Close(), "504")
	assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://storage.googleapis.com/upload/storage/v1/b/bucket/o"])
}
