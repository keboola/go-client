package keboola

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
)

type downloadConfig struct {
	transport http.RoundTripper
}

type DownloadOption func(c *downloadConfig)

func WithDownloadTransport(transport http.RoundTripper) DownloadOption {
	return func(c *downloadConfig) {
		c.transport = transport
	}
}

func Download(ctx context.Context, file *FileDownloadCredentials) ([]byte, error) {
	return DownloadSlice(ctx, file, "")
}

func DownloadManifest(ctx context.Context, file *FileDownloadCredentials) ([]byte, error) {
	return DownloadSlice(ctx, file, ManifestFileName)
}

func DownloadSlice(ctx context.Context, file *FileDownloadCredentials, slice string) (out []byte, err error) {
	reader, err := DownloadSliceReader(ctx, file, slice)
	if err != nil {
		return nil, err
	}
	out, err = io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func DownloadReader(ctx context.Context, file *FileDownloadCredentials) (io.ReadCloser, error) {
	return DownloadSliceReader(ctx, file, "")
}

func DownloadManifestReader(ctx context.Context, file *FileDownloadCredentials) (io.ReadCloser, error) {
	return DownloadSliceReader(ctx, file, ManifestFileName)
}

func DownloadSliceReader(ctx context.Context, file *FileDownloadCredentials, slice string, opts ...DownloadOption) (io.ReadCloser, error) {
	c := downloadConfig{}
	for _, opt := range opts {
		opt(&c)
	}
	switch file.Provider {
	case abs.Provider:
		return abs.NewDownloadReader(ctx, file.ABSDownloadParams, slice, c.transport)
	case gcs.Provider:
		return gcs.NewDownloadReader(ctx, file.GCSDownloadParams, slice, c.transport)
	case s3.Provider:
		return s3.NewDownloadReader(ctx, file.S3DownloadParams, file.Region, slice, c.transport)
	default:
		return nil, fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
}