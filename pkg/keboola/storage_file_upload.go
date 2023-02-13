package keboola

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"gocloud.dev/blob"

	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
)

type uploadConfig struct {
	transport http.RoundTripper
}

type UploadOption func(c *uploadConfig)

func WithUploadTransport(transport http.RoundTripper) UploadOption {
	return func(c *uploadConfig) {
		c.transport = transport
	}
}

// NewUploadWriter instantiates a Writer to the Storage given by cloud provider specified in the File resource.
func NewUploadWriter(ctx context.Context, file *FileUploadCredentials, opts ...UploadOption) (*blob.Writer, error) {
	return NewUploadSliceWriter(ctx, file, "", opts...)
}

// NewUploadSliceWriter instantiates a Writer to the Storage given by cloud provider specified in the File resource and to the specified slice.
func NewUploadSliceWriter(ctx context.Context, file *FileUploadCredentials, slice string, opts ...UploadOption) (*blob.Writer, error) {
	c := uploadConfig{}
	for _, opt := range opts {
		opt(&c)
	}
	switch file.Provider {
	case abs.Provider:
		return abs.NewUploadWriter(ctx, file.ABSUploadParams, slice, c.transport)
	case gcs.Provider:
		return gcs.NewUploadWriter(ctx, file.GCSUploadParams, slice, c.transport)
	case s3.Provider:
		return s3.NewUploadWriter(ctx, file.S3UploadParams, file.Region, slice, c.transport)
	default:
		return nil, fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
}

// Upload instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes there
// content of the reader.
func Upload(ctx context.Context, file *FileUploadCredentials, fr io.Reader) (written int64, err error) {
	return UploadSlice(ctx, file, "", fr)
}

// UploadSlice instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes
// content of the reader to the specified slice.
func UploadSlice(ctx context.Context, file *FileUploadCredentials, slice string, fr io.Reader) (written int64, err error) {
	bw, err := NewUploadSliceWriter(ctx, file, slice)
	if err != nil {
		return 0, fmt.Errorf("cannot open bucket writer: %w", err)
	}

	defer func() {
		if closeErr := bw.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("cannot close bucket writer: %w", closeErr)
		}
	}()

	return io.Copy(bw, fr)
}

// UploadSlicedFileManifest instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes
// content of the reader to the specified slice manifest.
func UploadSlicedFileManifest(ctx context.Context, file *FileUploadCredentials, slices []string) (written int64, err error) {
	manifest, err := NewSlicedFileManifest(file, slices)
	if err != nil {
		return 0, err
	}
	marshaledManifest, err := json.Marshal(manifest)
	if err != nil {
		return 0, err
	}

	return UploadSlice(ctx, file, ManifestFileName, bytes.NewReader(marshaledManifest))
}
