package s3_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/s3"
	"github.com/keboola/go-client/pkg/storageapi/testdata"
)

func TestCreateFileResourceAndUpload(t *testing.T) {
	t.Parallel()
	storageApiClient := storageapi.ClientForAnEmptyProject(t, testproject.WithStagingStorageS3())
	for _, tc := range testdata.UploadTestCases() {
		tc.Run(t, storageApiClient)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()

	f := &storageapi.File{
		Provider: "aws",
		S3UploadParams: &s3.UploadParams{
			Key:    "exp-15-files-4516-27298008-2022-11-08.test1",
			Bucket: "kbc-sapi-files",
		},
	}

	res, err := storageapi.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &storageapi.SlicedFileManifest{Entries: []storageapi.Slice{
		{Url: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1one"},
		{Url: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1two"},
	}}
	assert.Equal(t, e, res)
}

func TestTransportRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("PUT", `https://bucket.s3.us-east-1.amazonaws.com/key`, httpmock.NewStringResponder(504, "test"))

	params := &s3.UploadParams{
		Key:    "key",
		Bucket: "bucket",
		Credentials: s3.Credentials{
			AccessKeyId:     "accessKeyId",
			SecretAccessKey: "secretAccessKey",
			SessionToken:    "sessionToken",
			Expiration:      iso8601.Time{},
		},
		Acl: "private",
	}
	bw, err := s3.NewUploadWriter(context.Background(), params, "us-east-1", "", transport)
	assert.NoError(t, err)
	content := []byte("col1,col2\nval1,val2\n")
	_, err = bw.Write(content)
	assert.NoError(t, err)
	assert.ErrorContains(t, bw.Close(), "504")
	assert.Equal(t, 3, transport.GetCallCountInfo()["PUT https://bucket.s3.us-east-1.amazonaws.com/key"])
}
