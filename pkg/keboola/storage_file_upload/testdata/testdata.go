package testdata

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
)

type UploadTestCase struct {
	Permanent bool
	Sliced    bool
	Encrypted bool
	Gzipped   bool
}

func UploadAndDownloadTestCases() (out []UploadTestCase) {
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

func (tc UploadTestCase) Run(t *testing.T, api *keboola.AuthorizedAPI) {
	ctx := context.Background()

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	t.Helper()
	t.Run(tc.Name(), func(t *testing.T) {
		t.Parallel()

		// Content
		content := []byte("col1,col2\nval1,val2\n")

		// Create file resource
		opts := []keboola.CreateFileOption{
			keboola.WithIsPermanent(tc.Permanent),
			keboola.WithIsSliced(tc.Sliced),
			keboola.WithTags("tag1", "tag2"),
		}
		if !tc.Encrypted {
			opts = append(opts, keboola.WithDisableEncryption())
		}
		file, err := api.CreateFileResourceRequest(defBranch.ID, "test", opts...).Send(ctx)
		require.NoError(t, err)

		// Assert common fields
		assert.NotEmpty(t, file.FileID)
		assert.NotEmpty(t, file.URL)
		assert.NotEmpty(t, file.Created)
		assert.Equal(t, tc.Permanent, file.IsPermanent)
		assert.Equal(t, tc.Sliced, file.IsSliced)
		assert.Equal(t, []string{"tag1", "tag2"}, file.Tags)

		// Assert credentials expiration
		now := time.Now()
		expiration := file.CredentialsExpiration()
		assert.True(t, expiration.After(now), expiration.String())

		// Assert provider specific fields
		switch file.Provider {
		case abs.Provider:
			assert.Equal(t, true, file.IsEncrypted)
			assert.NotEmpty(t, file.ABSUploadParams)
			assert.NotEmpty(t, file.ABSUploadParams.BlobName)
			assert.NotEmpty(t, file.ABSUploadParams.Credentials.SASConnectionString)
		case gcs.Provider:
			assert.Equal(t, true, file.IsEncrypted)
			assert.NotEmpty(t, file.GCSUploadParams)
			assert.NotEmpty(t, file.GCSUploadParams.AccessToken)
			assert.NotEmpty(t, file.GCSUploadParams.Bucket)
			assert.NotEmpty(t, file.GCSUploadParams.Key)
			assert.NotEmpty(t, file.GCSUploadParams.TokenType)
		case s3.Provider:
			assert.Equal(t, tc.Encrypted, file.IsEncrypted)
			assert.NotEmpty(t, file.S3UploadParams)
			assert.NotEmpty(t, file.S3UploadParams.Bucket)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.AccessKeyID)
			assert.NotEmpty(t, file.S3UploadParams.Credentials.SecretAccessKey)
		default:
			panic(fmt.Errorf(`unexpected storage provider "%s"`, file.Provider))
		}

		// Upload
		if tc.Gzipped {
			// Create upload writer
			var bw *blob.Writer
			var err error
			if tc.Sliced {
				bw, err = keboola.NewUploadSliceWriter(ctx, file, "slice1")
			} else {
				bw, err = keboola.NewUploadWriter(ctx, file)
			}
			require.NoError(t, err)

			// Wrap the writer with the gzip writer
			gzw := gzip.NewWriter(bw)

			// Upload
			written, err := gzw.Write(content)
			require.NoError(t, err)
			assert.Equal(t, len(content), written)
			assert.NoError(t, gzw.Close())
			assert.NoError(t, bw.Close())
		} else {
			// Upload from reader
			var written int64
			var err error
			if tc.Sliced {
				written, err = keboola.UploadSlice(ctx, file, "slice1", bytes.NewReader(content))
			} else {
				written, err = keboola.Upload(ctx, file, bytes.NewReader(content))
			}
			require.NoError(t, err)
			assert.Equal(t, int64(len(content)), written)
		}

		// Upload manifest
		if tc.Sliced {
			_, err := keboola.UploadSlicedFileManifest(ctx, file, []string{"slice1"})
			require.NoError(t, err)
		}

		// Get file download credentials
		credentials, err := api.GetFileWithCredentialsRequest(file.FileKey).Send(ctx)
		require.NoError(t, err)

		// Request file content
		if tc.Sliced {
			// Check manifest content
			slicesList, err := keboola.DownloadManifest(ctx, credentials)
			require.NoError(t, err)
			assert.Len(t, slicesList, 1)
			assert.Equal(t, "slice1", slicesList[0])

			// Check slice attributes
			attrs, err := keboola.GetFileAttributes(ctx, credentials, "slice1")
			require.NoError(t, err)
			assert.NotEmpty(t, attrs.ModTime)
			if tc.Gzipped {
				assert.Equal(t, "application/x-gzip", attrs.ContentType)
			} else {
				assert.Equal(t, "text/plain; charset=utf-8", attrs.ContentType)
			}
			assert.NotZero(t, attrs.Size)

			// Read slice
			var reader io.ReadCloser
			sliceReader, err := keboola.DownloadSliceReader(ctx, credentials, "slice1")
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, sliceReader.Close())
			}()

			// Decode
			if tc.Gzipped {
				gzipReader, err := gzip.NewReader(sliceReader)
				require.NoError(t, err)
				defer func() {
					assert.NoError(t, gzipReader.Close())
				}()
				reader = gzipReader
			} else {
				reader = sliceReader
			}

			// Read slice
			fileContent, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, content, fileContent)
		} else {
			// Check attributes
			attrs, err := keboola.GetFileAttributes(ctx, credentials, "")
			require.NoError(t, err)
			assert.NotEmpty(t, attrs.ModTime)
			if tc.Gzipped {
				assert.Equal(t, "application/x-gzip", attrs.ContentType)
			} else {
				assert.Equal(t, "text/plain; charset=utf-8", attrs.ContentType)
			}
			assert.NotZero(t, attrs.Size)

			var reader io.ReadCloser
			fileReader, err := keboola.DownloadReader(ctx, credentials)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, fileReader.Close())
			}()

			// Decode
			if tc.Gzipped {
				gzipReader, err := gzip.NewReader(fileReader)
				require.NoError(t, err)
				defer func() {
					assert.NoError(t, gzipReader.Close())
				}()
				reader = gzipReader
			} else {
				reader = fileReader
			}

			// Read file
			fileContent, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, content, fileContent)
		}
	})
}
