package keboola

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"

	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
)

type File struct {
	ID              int               `json:"id" readonly:"true"`
	Created         iso8601.Time      `json:"created" readonly:"true"`
	IsSliced        bool              `json:"isSliced,omitempty"`
	IsEncrypted     bool              `json:"isEncrypted,omitempty"`
	Name            string            `json:"name"`
	URL             string            `json:"url" readonly:"true"`
	Provider        string            `json:"provider" readonly:"true"`
	Region          string            `json:"region" readonly:"true"`
	SizeBytes       uint64            `json:"sizeBytes,omitempty"`
	Tags            []string          `json:"tags,omitempty"`
	MaxAgeDays      uint              `json:"maxAgeDays" readonly:"true"`
	ABSUploadParams *abs.UploadParams `json:"absUploadParams,omitempty" readonly:"true"`
	GCSUploadParams *gcs.UploadParams `json:"gcsUploadParams,omitempty" readonly:"true"`
	S3UploadParams  *s3.UploadParams  `json:"uploadParams,omitempty" readonly:"true"`

	ContentType     string `json:"contentType,omitempty"`
	FederationToken bool   `json:"federationToken,omitempty"`
	IsPermanent     bool   `json:"isPermanent,omitempty"`
	Notify          bool   `json:"notify,omitempty"`
}

type SlicedFileManifest struct {
	Entries []Slice `json:"entries"`
}

type Slice struct {
	URL string `json:"url"`
}

// CreateFileResourceRequest https://keboola.docs.apiary.io/#reference/files/upload-file/create-file-resource
func (a *API) CreateFileResourceRequest(file *File) client.APIRequest[*File] {
	file.FederationToken = true
	request := a.
		newRequest(StorageAPI).
		WithResult(file).
		WithPost("files/prepare").
		WithFormBody(client.ToFormBody(client.StructToMap(file, nil)))
	return client.NewAPIRequest(file, request)
}

// ListFilesRequest https://keboola.docs.apiary.io/#reference/files/list-files
func (a *API) ListFilesRequest() client.APIRequest[*[]*File] {
	var files []*File
	request := a.
		newRequest(StorageAPI).
		WithResult(&files).
		WithGet("files").
		AndQueryParam("limit", "200").
		WithOnSuccess(func(_ context.Context, _ client.HTTPResponse) error {
			sort.Slice(files, func(i, j int) bool {
				return files[i].ID < files[j].ID
			})
			return nil
		})
	return client.NewAPIRequest(&files, request)
}

// GetFileRequest https://keboola.docs.apiary.io/#reference/files/manage-files/file-detail
func (a *API) GetFileRequest(id int) client.APIRequest[*File] {
	file := &File{}
	request := a.
		newRequest(StorageAPI).
		WithResult(file).
		WithGet("files/{fileId}").
		AndPathParam("fileId", strconv.Itoa(id))
	return client.NewAPIRequest(file, request)
}

// DeleteFileRequest https://keboola.docs.apiary.io/#reference/files/manage-files/delete-file
func (a *API) DeleteFileRequest(id int) client.APIRequest[client.NoResult] {
	request := a.
		newRequest(StorageAPI).
		WithDelete("files/{fileId}").
		WithOnError(func(ctx context.Context, response client.HTTPResponse, err error) error {
			// Metadata about files are stored in the ElasticSearch, operations may not be reflected immediately.
			if response.StatusCode() == http.StatusNotFound {
				return nil
			}
			return err
		}).
		AndPathParam("fileId", strconv.Itoa(id))
	return client.NewAPIRequest(client.NoResult{}, request)
}

type uploadConfig struct {
	transport http.RoundTripper
}

type UploadOptions func(c *uploadConfig)

func WithUploadTransport(transport http.RoundTripper) UploadOptions {
	return func(c *uploadConfig) {
		c.transport = transport
	}
}

// NewUploadWriter instantiates a Writer to the Storage given by cloud provider specified in the File resource.
func NewUploadWriter(ctx context.Context, file *File, opts ...UploadOptions) (*blob.Writer, error) {
	return NewUploadSliceWriter(ctx, file, "", opts...)
}

// NewUploadSliceWriter instantiates a Writer to the Storage given by cloud provider specified in the File resource and to the specified slice.
func NewUploadSliceWriter(ctx context.Context, file *File, slice string, opts ...UploadOptions) (*blob.Writer, error) {
	uploadConfig := uploadConfig{}
	for _, opt := range opts {
		opt(&uploadConfig)
	}
	switch file.Provider {
	case abs.Provider:
		return abs.NewUploadWriter(ctx, file.ABSUploadParams, slice, uploadConfig.transport)
	case gcs.Provider:
		return gcs.NewUploadWriter(ctx, file.GCSUploadParams, slice, uploadConfig.transport)
	case s3.Provider:
		return s3.NewUploadWriter(ctx, file.S3UploadParams, file.Region, slice, uploadConfig.transport)
	default:
		return nil, fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
}

// Upload instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes there
// content of the reader.
func Upload(ctx context.Context, file *File, fr io.Reader) (written int64, err error) {
	return UploadSlice(ctx, file, "", fr)
}

// UploadSlice instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes
// content of the reader to the specified slice.
func UploadSlice(ctx context.Context, file *File, slice string, fr io.Reader) (written int64, err error) {
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
func UploadSlicedFileManifest(ctx context.Context, file *File, slices []string) (written int64, err error) {
	manifest, err := NewSlicedFileManifest(file, slices)
	if err != nil {
		return 0, err
	}
	marshaledManifest, err := json.Marshal(manifest)
	if err != nil {
		return 0, err
	}

	return UploadSlice(ctx, file, "manifest", bytes.NewReader(marshaledManifest))
}

func NewSliceURL(file *File, slice string) (string, error) {
	switch file.Provider {
	case abs.Provider:
		return abs.NewSliceURL(file.ABSUploadParams, slice), nil
	case gcs.Provider:
		return gcs.NewSliceURL(file.GCSUploadParams, slice), nil
	case s3.Provider:
		return s3.NewSliceURL(file.S3UploadParams, slice), nil
	default:
		return "", fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
}

func NewSlicedFileManifest(file *File, sliceNames []string) (*SlicedFileManifest, error) {
	m := &SlicedFileManifest{Entries: make([]Slice, 0)}
	for _, s := range sliceNames {
		url, err := NewSliceURL(file, s)
		if err != nil {
			return nil, err
		}
		m.Entries = append(m.Entries, Slice{URL: url})
	}
	return m, nil
}