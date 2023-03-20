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

// DownloadAll downloads all slices of a file. To download a whole file, use `Download` instead.
//
// This downloads each individual slice and concatenates them into a single byte slice.
// That means this may end up using a lot of memory if your files are large. If you believe
// this may end up being the case, then use `DownloadManifest` followed by downloading
// each slice using `DownloadSlice` or `DownloadSliceReader`.
func DownloadAll(ctx context.Context, file *FileDownloadCredentials) ([]byte, error) {
	if !file.IsSliced {
		return nil, fmt.Errorf("cannot download a whole file as a sliced file")
	}

	out := []byte{}

	slices, err := DownloadManifest(ctx, file)
	if err != nil {
		return nil, err
	}

	for _, slice := range slices {
		data, err := DownloadSlice(ctx, file, slice)
		if err != nil {
			return nil, err
		}
		out = append(out, data...)
	}

	return out, nil
}

// Download downloads a whole file. To download sliced files, use `DownloadAll` instead.
func Download(ctx context.Context, file *FileDownloadCredentials) ([]byte, error) {
	if file.IsSliced {
		return nil, fmt.Errorf("cannot download a sliced file as a whole file")
	}
	return DownloadSlice(ctx, file, "")
}

// DownloadManifest downloads the file manifest, which contains a list of slices.
//
// This function assumes that `file.IsSliced == true`.
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

// DownloadSlice downloads a single slice and returns it whole as a byte slice.
//
// For streaming data, prefer `DownloadSliceReader`.
//
// This function assumes that `file.IsSliced == true`.
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

// DownloadReader returns a reader which may be used to download a whole file.
//
// This functions assumes that `file.IsSliced == false`.
func DownloadReader(ctx context.Context, file *FileDownloadCredentials) (io.ReadCloser, error) {
	return DownloadSliceReader(ctx, file, "")
}

// DownloadManifestReader returns a reader which may be used to download the manifest of a sliced file.
//
// This function assumes that `file.IsSliced == true`.
func DownloadManifestReader(ctx context.Context, file *FileDownloadCredentials) (io.ReadCloser, error) {
	return DownloadSliceReader(ctx, file, ManifestFileName)
}

// DownloadSliceReader returns a reader which may be used to download a single slice of a file.
//
// The `DownloadManifest` family of functions may be used to obtain a manifest which contains the list of slices.
//
// This function assumes that `file.IsSliced == true`.
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

// GetFileAttributes returns information about a while, such as its content type, last changed time, and byte size.
//
// If `file.IsSliced == false`, the `slice` parameter should be an empty string.
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
