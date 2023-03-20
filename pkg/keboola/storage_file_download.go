package keboola

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"gocloud.dev/blob"

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
	if file.IsSliced {
		return nil, fmt.Errorf("cannot download a sliced file as a whole file")
	}
	return DownloadSlice(ctx, file, "")
}

func DownloadManifest(ctx context.Context, file *FileDownloadCredentials) (SlicesList, error) {
	rawManifest, err := DownloadSlice(ctx, file, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("cannot download manifest: %w", err)
	}

	manifest := &SlicedFileManifest{}
	err = json.Unmarshal(rawManifest, manifest)
	if err != nil {
		return nil, fmt.Errorf("cannot map file contents to manifest: %w", err)
	}

	dstURL, err := file.DestinationURL()
	if err != nil {
		return nil, err
	}
	res := SlicesList{}
	for _, slice := range manifest.Entries {
		if !strings.HasPrefix(slice.URL, dstURL) {
			return nil, fmt.Errorf(`slice URL "%s" seems wrong: %w`, slice.URL, err)
		}
		res = append(res, strings.TrimPrefix(slice.URL, dstURL))
	}
	return res, nil
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

func GetFileAttributes(ctx context.Context, file *FileDownloadCredentials, slice string, opts ...DownloadOption) (*FileAttributes, error) {
	c := downloadConfig{}
	for _, opt := range opts {
		opt(&c)
	}
	var attrs *blob.Attributes
	var err error
	switch file.Provider {
	case abs.Provider:
		attrs, err = abs.GetFileAttributes(ctx, file.ABSDownloadParams, slice, c.transport)
	case gcs.Provider:
		attrs, err = gcs.GetFileAttributes(ctx, file.GCSDownloadParams, slice, c.transport)
	case s3.Provider:
		attrs, err = s3.GetFileAttributes(ctx, file.S3DownloadParams, file.Region, slice, c.transport)
	default:
		return nil, fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
	if err != nil {
		return nil, err
	}
	return &FileAttributes{
		ContentType: attrs.ContentType,
		ModTime:     attrs.ModTime,
		Size:        attrs.Size,
	}, nil
}
