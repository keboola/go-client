package gcs_test

import (
	"testing"

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
		GCSUploadParams: gcs.UploadParams{
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
