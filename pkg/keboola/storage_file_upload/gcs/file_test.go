package gcs_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/testdata"
)

func TestUploadAndDownload(t *testing.T) {
	t.Parallel()
	_, api := keboola.APIClientForAnEmptyProject(t, context.Background(), testproject.WithStagingStorageGCS())
	for _, tc := range testdata.UploadAndDownloadTestCases() {
		tc.Run(t, api)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()

	f := &keboola.FileUploadCredentials{
		File: keboola.File{
			Provider: "gcp",
		},
		GCSUploadParams: &gcs.UploadParams{
			Path: gcs.Path{
				Key:    "exp-15-files-4516-27298008-2022-11-08.test1",
				Bucket: "kbc-sapi-files",
			},
		},
	}

	res, err := keboola.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &keboola.SlicedFileManifest{Entries: []keboola.Slice{
		{URL: "gs://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1one"},
		{URL: "gs://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1two"},
	}}
	assert.Equal(t, e, res)
}

func TestTransportRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("POST", `https://storage.googleapis.com/upload/storage/v1/b/bucket/o`, httpmock.NewStringResponder(504, "test"))

	params := &gcs.UploadParams{
		Path: gcs.Path{
			Key:    "key",
			Bucket: "bucket",
		},
		Credentials: gcs.Credentials{
			ProjectID:   "project",
			AccessToken: "token",
			TokenType:   "Bearer",
			ExpiresIn:   0,
		},
	}
	bw, err := gcs.NewUploadWriter(context.Background(), params, "", transport)
	assert.NoError(t, err)
	content := []byte("col1,col2\nval1,val2\n")
	_, err = bw.Write(content)
	assert.NoError(t, err)
	assert.ErrorContains(t, bw.Close(), "504")
	assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://storage.googleapis.com/upload/storage/v1/b/bucket/o"])
}
