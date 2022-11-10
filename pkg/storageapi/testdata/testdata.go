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
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob"
)

type UploadTestCase struct {
	Permanent bool
	Sliced    bool
	Encrypted bool
	Gzipped   bool
}

func UploadTestCases() (out []UploadTestCase) {
	// Test matrix, all combinations of attributes
	for flags := 0b0000; flags <= 0b1111; flags++ {
		out = append(out, UploadTestCase{
			Permanent: flags&0b0001 != 0,
			Sliced:    flags&0b0010 != 0,
			Encrypted: flags&0b0100 != 0,
			Gzipped:   flags&0b1000 != 0,
		})
	}
	return out
}

func (tc UploadTestCase) Name() string {
	return fmt.Sprintf("permanent[%t]_sliced[%t]_encrypted[%t]_gzipped[%t]", tc.Permanent, tc.Sliced, tc.Encrypted, tc.Gzipped)
}

func (tc UploadTestCase) Run(t *testing.T, storageApiClient client.Sender) {
	t.Helper()
	t.Run(tc.Name(), func(t *testing.T) {
		t.Parallel()

		// Content
		content := []byte("col1,col2\nval1,val2\n")

		// Create file definition
		file := &storageapi.File{
			IsPermanent: tc.Permanent,
			IsSliced:    tc.Sliced,
			IsEncrypted: tc.Encrypted,
			Name:        "test",
			Tags:        []string{"tag1", "tag2"},
		}

		// Create file resource
		ctx := context.Background()
		_, err := storageapi.CreateFileResourceRequest(file).Send(ctx, storageApiClient)
		assert.NoError(t, err)

		// Assert common fields
		assert.NotEmpty(t, file.ID)
		assert.NotEmpty(t, file.Url)
		assert.NotEmpty(t, file.Created)
		assert.Equal(t, tc.Permanent, file.IsPermanent)
		assert.Equal(t, tc.Sliced, file.IsSliced)
		assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)

		// Assert provider specific fields
		switch file.Provider {
		case s3.Provider:
			assert.Equal(t, tc.Encrypted, file.IsEncrypted)
			assert.NotEmpty(t, file.S3UploadParams)
			assert.NotEmpty(t, file.S3UploadParams.Bucket)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.AccessKeyId)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.SecretAccessKey)
		case abs.Provider:
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
			var bw *blob.Writer
			var err error
			if tc.Sliced {
				bw, err = storageapi.NewUploadSliceWriter(ctx, file, "slice1")
			} else {
				bw, err = storageapi.NewUploadWriter(ctx, file)
			}
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
			var written int64
			var err error
			if tc.Sliced {
				written, err = storageapi.UploadSlice(ctx, file, "slice1", bytes.NewReader(content))
			} else {
				written, err = storageapi.Upload(ctx, file, bytes.NewReader(content))
			}
			assert.NoError(t, err)
			assert.Equal(t, int64(len(content)), written)
		}

		// Upload manifest
		if tc.Sliced {
			_, err := storageapi.UploadSlicedFileManifest(ctx, file, []string{"slice1"})
			assert.NoError(t, err)
		}

		// Get file resource
		fileFromRequest, err := storageapi.GetFileResourceRequest(file.ID).Send(ctx, storageApiClient)
		assert.NoError(t, err)

		// Request file content
		resp, err := http.Get(fileFromRequest.Url)
		assert.NoError(t, err)
		defer resp.Body.Close()

		// Check that we didn't get error instead of the file
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			t.Logf("Response body: \n%s\n", data)
		}

		// Get file reader
		var fileReader io.Reader
		if tc.Gzipped && !tc.Sliced {
			fileReader, err = gzip.NewReader(resp.Body)
			assert.NoError(t, err)
		} else {
			fileReader = resp.Body
		}

		// Get and compare file content
		fileContent, err := io.ReadAll(fileReader)
		assert.NoError(t, err)
		if tc.Sliced {
			// Check manifest content
			wildcards.Assert(t, `{"entries":[{"url":"%sslice1"}]}`, string(fileContent))
		} else {
			// Check file content
			assert.Equal(t, content, fileContent)
		}
	})
}
