package s3_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/testdata"
)

func TestCreateFileResourceAndUpload(t *testing.T) {
	t.Parallel()
	storageApiClient := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageS3())
	for _, tc := range testdata.UploadTestCases() {
		tc.Run(t, storageApiClient)
	}
}
