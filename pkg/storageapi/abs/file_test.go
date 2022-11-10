package abs_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/abs"
	"github.com/keboola/go-client/pkg/storageapi/testdata"
)

func TestCreateFileResourceAndUpload(t *testing.T) {
	t.Parallel()
	storageApiClient := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageABS())
	for _, tc := range testdata.UploadTestCases() {
		tc.Run(t, storageApiClient)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()

	f := &storageapi.File{
		Provider: "azure",
		ABSUploadParams: abs.UploadParams{
			BlobName:    "test1",
			AccountName: "kbcfshc7chguaeh2km",
			Container:   "exp-15-files-4516-27298008-2022-11-08",
		},
	}

	res, err := storageapi.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &storageapi.SlicedFileManifest{Entries: []storageapi.Slice{
		{Url: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1one"},
		{Url: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1two"},
	}}
	assert.Equal(t, e, res)
}
