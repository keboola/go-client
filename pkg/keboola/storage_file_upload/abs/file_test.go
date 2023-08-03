package abs_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/testdata"
)

func TestUploadAndDownload(t *testing.T) {
	t.Parallel()
	_, api := keboola.APIClientForAnEmptyProject(t, context.Background(), testproject.WithStagingStorageABS())
	for _, tc := range testdata.UploadAndDownloadTestCases() {
		tc.Run(t, api)
	}
}

func TestCreateImportManifest(t *testing.T) {
	t.Parallel()
	f := &keboola.FileUploadCredentials{
		File: keboola.File{
			Provider: "azure",
		},
		ABSUploadParams: &abs.UploadParams{
			BlobName:    "test1",
			AccountName: "kbcfshc7chguaeh2km",
			Container:   "exp-15-files-4516-27298008-2022-11-08",
		},
	}

	res, err := keboola.NewSlicedFileManifest(f, []string{"one", "two"})
	assert.NoError(t, err)

	e := &keboola.SlicedFileManifest{Entries: []keboola.Slice{
		{URL: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1one"},
		{URL: "azure://kbcfshc7chguaeh2km.blob.core.windows.net/exp-15-files-4516-27298008-2022-11-08/test1two"},
	}}
	assert.Equal(t, e, res)
}

func TestTransportRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("PUT", `https://example.com/container/blob`, httpmock.NewStringResponder(504, "test"))

	params := &abs.UploadParams{
		BlobName:    "blob",
		AccountName: "account",
		Container:   "container",
		Credentials: abs.Credentials{
			SASConnectionString: "BlobEndpoint=https://example.com;SharedAccessSignature=sas",
			Expiration:          iso8601.Time{},
		},
	}
	bw, err := abs.NewUploadWriter(context.Background(), params, "", transport)
	assert.NoError(t, err)
	content := []byte("col1,col2\nval1,val2\n")
	_, err = bw.Write(content)
	assert.NoError(t, err)
	assert.ErrorContains(t, bw.Close(), "504")
	assert.Equal(t, 4, transport.GetCallCountInfo()["PUT https://example.com/container/blob"])
}
