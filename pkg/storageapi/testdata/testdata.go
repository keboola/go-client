package testdata

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/abs"
	"github.com/keboola/go-client/pkg/storageapi/s3"
	"github.com/stretchr/testify/assert"
)

type UploadTestCase struct {
	Public    bool
	Pernament bool
	Sliced    bool
	Encrypted bool
	Gzipped   bool
}

func UploadTestCases() (out []UploadTestCase) {
	for _, public := range []bool{true, false} {
		for _, permanent := range []bool{true, false} {
			for _, sliced := range []bool{true, false} {
				for _, encrypted := range []bool{true, false} {
					for _, gzipped := range []bool{true, false} {
						out = append(out, UploadTestCase{
							Public:    public,
							Pernament: permanent,
							Sliced:    sliced,
							Encrypted: encrypted,
							Gzipped:   gzipped,
						})
					}
				}
			}
		}
	}
	return out
}

func (tc UploadTestCase) Name() string {
	return fmt.Sprintf("public[%t]_permanent[%t]_sliced[%t]_encrypted[%t]_gzipped[%t]", tc.Public, tc.Pernament, tc.Sliced, tc.Encrypted, tc.Gzipped)
}

func (tc UploadTestCase) Run(t *testing.T, storageApiClient client.Sender) {
	t.Run(tc.Name(), func(t *testing.T) {
		t.Parallel()

		// Content
		content := []byte("col1,col2\nval1,val2\n")

		// Create file definition
		file := &storageapi.File{
			IsPublic:    tc.Public,
			IsPermanent: tc.Pernament,
			IsSliced:    tc.Sliced,
			IsEncrypted: tc.Encrypted,
			Name:        "test",
			Tags:        []string{"tag1", "tag2"},
		}

		// TODO
		if tc.Sliced {
			t.Skipf("sliced file is not supported yet")
		}

		// Create file resource
		ctx := context.Background()
		_, err := storageapi.CreateFileResourceRequest(file).Send(ctx, storageApiClient)
		assert.NoError(t, err)

		// Assert common fields
		assert.NotEmpty(t, file.ID)
		assert.NotEmpty(t, file.Url)
		assert.NotEmpty(t, file.Created)
		assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)

		// Assert provider specific fields
		switch file.Provider {
		case s3.Provider:
			assert.Equal(t, tc.Pernament, file.IsPermanent)
			assert.Equal(t, tc.Sliced, file.IsSliced)
			assert.NotEmpty(t, file.S3UploadParams)
			assert.NotEmpty(t, file.S3UploadParams.Bucket)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.AccessKeyId)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.SecretAccessKey)
		case abs.Provider:
			assert.Equal(t, false, file.IsPublic)
			assert.Equal(t, true, file.IsEncrypted)
			assert.NotEmpty(t, file.ABSUploadParams)
			assert.NotEmpty(t, file.ABSUploadParams.BlobName)
			assert.NotEmpty(t, file.ABSUploadParams.Credentials.SASConnectionString)
		default:
			panic(fmt.Errorf(`unexpected storage provider "%s"`, file.Provider))
		}

		// Upload
		if tc.Gzipped {
			// Create upload writer
			bw, err := storageapi.NewUploadWriter(ctx, file)
			assert.NoError(t, err)

			// Wrap the writer with the gzip writer
			gzw := gzip.NewWriter(bw)

			// Upload
			written, err := gzw.Write(content)
			assert.NoError(t, err)
			assert.Equal(t, len(content), written)
			assert.NoError(t, gzw.Close())
			assert.NoError(t, bw.Close())
		} else {
			// Upload from reader
			written, err := storageapi.Upload(ctx, file, bytes.NewReader(content))
			assert.NoError(t, err)
			assert.Equal(t, int64(len(content)), written)
		}

		// Get file resource
		file, err = storageapi.GetFileResourceRequest(file.ID).Send(ctx, storageApiClient)
		assert.NoError(t, err)

		// Request file content
		resp, err := http.Get(file.Url)
		assert.NoError(t, err)
		defer resp.Body.Close()

		// Get file reader
		var fileReader io.Reader
		if tc.Gzipped {
			fileReader, err = gzip.NewReader(resp.Body)
			assert.NoError(t, err)
		} else {
			fileReader = resp.Body
		}

		// Get and compare file content
		fileContent, err := io.ReadAll(fileReader)
		assert.NoError(t, err)
		assert.Equal(t, content, fileContent)
	})
}
