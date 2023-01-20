package s3_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/testdata"
)

func TestCreateFileResourceAndUpload(t *testing.T) {
	t.Parallel()
	api := keboola.APIClientForAnEmptyProject(t, context.Background(), testproject.WithStagingStorageS3())
	for _, tc := range testdata.UploadTestCases() {
		tc.Run(t, api)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()

	f := &keboola.File{
		Provider: "aws",
		S3UploadParams: &s3.UploadParams{
			Key:    "exp-15-files-4516-27298008-2022-11-08.test1",
			Bucket: "kbc-sapi-files",
		},
	}

	res, err := keboola.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &keboola.SlicedFileManifest{Entries: []keboola.Slice{
		{URL: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1one"},
		{URL: "s3://kbc-sapi-files/exp-15-files-4516-27298008-2022-11-08.test1two"},
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
			AccessKeyID:     "accessKeyId",
			SecretAccessKey: "secretAccessKey",
			SessionToken:    "sessionToken",
			Expiration:      iso8601.Time{},
		},
		ACL: "private",
	}
	bw, err := s3.NewUploadWriter(context.Background(), params, "us-east-1", "", transport)
	assert.NoError(t, err)
	content := []byte("col1,col2\nval1,val2\n")
	_, err = bw.Write(content)
	assert.NoError(t, err)
	assert.ErrorContains(t, bw.Close(), "504")
	assert.Equal(t, 3, transport.GetCallCountInfo()["PUT https://bucket.s3.us-east-1.amazonaws.com/key"])
}
