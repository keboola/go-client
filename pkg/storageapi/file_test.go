package storageapi_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/abs"
	"github.com/keboola/go-client/pkg/storageapi/s3"
)

func TestCreateImportManifestOnABS(t *testing.T) {
	t.Parallel()

	f := &storageapi.File{
		Provider: "azure",
		ABSUploadParams: abs.UploadParams{
			BlobName:    "test1",
			AccountName: "kbcfshc7chguaeh2km",
			Container:   "exp-15-files-4516-27298008-2022-11-08",
		},
	}

	res, err := storageapi.CreateSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &storageapi.SlicedFileManifest{Entries: []*storageapi.ImportManifestEntry{
		{Url: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1one"},
		{Url: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1two"},
	}}
	assert.Equal(t, e, res)
}

func TestCreateImportManifestOnS3(t *testing.T) {
	t.Parallel()

	f := &storageapi.File{
		Provider: "aws",
		S3UploadParams: s3.UploadParams{
			Key:    "exp-15-files-4516-27298008-2022-11-08.test1",
			Bucket: "kbc-sapi-files",
		},
	}

	res, err := storageapi.CreateSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &storageapi.SlicedFileManifest{Entries: []*storageapi.ImportManifestEntry{
		{Url: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1one"},
		{Url: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1two"},
	}}
	assert.Equal(t, e, res)
}
