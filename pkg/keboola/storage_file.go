package keboola

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/abs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
)

const ManifestFileName = "manifest"

type File struct {
	ID              int          `json:"id" readonly:"true"`
	Created         iso8601.Time `json:"created" readonly:"true"`
	IsSliced        bool         `json:"isSliced,omitempty"`
	IsEncrypted     bool         `json:"isEncrypted,omitempty"`
	Name            string       `json:"name"`
	URL             string       `json:"url" readonly:"true"`
	Provider        string       `json:"provider" readonly:"true"`
	Region          string       `json:"region" readonly:"true"`
	SizeBytes       uint64       `json:"sizeBytes,omitempty"`
	Tags            []string     `json:"tags,omitempty"`
	MaxAgeDays      uint         `json:"maxAgeDays" readonly:"true"`
	ContentType     string       `json:"contentType,omitempty"`
	FederationToken bool         `json:"federationToken,omitempty"`
	IsPermanent     bool         `json:"isPermanent,omitempty"`
	Notify          bool         `json:"notify,omitempty"`
}
type S3DownloadParams = s3.DownloadParams

type ABSDownloadParams = abs.DownloadParams

type GCSDownloadParams = gcs.DownloadParams

type FileUploadCredentials struct {
	File
	ABSUploadParams *abs.UploadParams `json:"absUploadParams,omitempty"`
	GCSUploadParams *gcs.UploadParams `json:"gcsUploadParams,omitempty"`
	S3UploadParams  *s3.UploadParams  `json:"uploadParams,omitempty"`
}

type FileDownloadCredentials struct {
	File
	*S3DownloadParams
	*ABSDownloadParams
	*GCSDownloadParams
}

func (f *FileDownloadCredentials) DestinationURL() (string, error) {
	switch f.Provider {
	case abs.Provider:
		return f.ABSDownloadParams.DestinationURL()
	case gcs.Provider:
		return f.GCSDownloadParams.DestinationURL()
	case s3.Provider:
		return f.S3DownloadParams.DestinationURL()
	default:
		return "", fmt.Errorf(`unsupported provider "%s"`, f.Provider)
	}
}

type FileAttributes struct {
	ContentType string
	ModTime     time.Time
	Size        int64
}

type SlicesList []string

type SlicedFileManifest struct {
	Entries []Slice `json:"entries"`
}

type Slice struct {
	URL string `json:"url"`
}

type createFileConfig struct {
	name              string
	sizeBytes         uint64
	contentType       string
	isPermanent       bool
	notify            bool
	tags              []string
	isSliced          bool
	disableEncryption bool
}

type CreateFileOption interface {
	applyCreateFileOption(c *createFileConfig)
}

type withSizeBytes uint64

func WithSizeBytes(v uint64) withSizeBytes {
	return withSizeBytes(v)
}

func (v withSizeBytes) applyCreateFileOption(c *createFileConfig) {
	c.sizeBytes = uint64(v)
}

type withContentType string

func WithContentType(v string) withContentType {
	return withContentType(v)
}

func (v withContentType) applyCreateFileOption(c *createFileConfig) {
	c.contentType = string(v)
}

type withIsPermanent bool

func WithIsPermanent(v bool) withIsPermanent {
	return withIsPermanent(v)
}

func (v withIsPermanent) applyCreateFileOption(c *createFileConfig) {
	c.isPermanent = bool(v)
}

type withNotify bool

func WithNotify(v bool) withNotify {
	return withNotify(v)
}

func (v withNotify) applyCreateFileOption(c *createFileConfig) {
	c.notify = bool(v)
}

type withTags []string

func WithTags(v ...string) withTags {
	return withTags(v)
}

func (v withTags) applyCreateFileOption(c *createFileConfig) {
	c.tags = append(c.tags, v...)
}

type withIsSliced bool

func WithIsSliced(v bool) withIsSliced {
	return withIsSliced(v)
}

func (v withIsSliced) applyCreateFileOption(c *createFileConfig) {
	c.isSliced = bool(v)
}

type withDisableEncryption bool

func WithDisableEncryption() withDisableEncryption {
	return withDisableEncryption(true)
}

func (v withDisableEncryption) applyCreateFileOption(c *createFileConfig) {
	c.disableEncryption = bool(v)
}

func (c *createFileConfig) toMap() map[string]any {
	m := map[string]any{"name": c.name, "federationToken": true, "isEncrypted": true}
	if c.sizeBytes > 0 {
		m["sizeBytes"] = c.sizeBytes
	}
	if len(c.contentType) > 0 {
		m["contentType"] = c.contentType
	}
	if c.isPermanent {
		m["isPermanent"] = true
	}
	if c.notify {
		m["notify"] = true
	}
	if len(c.tags) > 0 {
		for i, tag := range c.tags {
			m[fmt.Sprintf("tags[%d]", i)] = tag
		}
	}
	if c.isSliced {
		m["isSliced"] = true
	}
	if c.disableEncryption {
		m["isEncrypted"] = false
	}
	return m
}

// CreateFileResourceRequest https://keboola.docs.apiary.io/#reference/files/upload-file/create-file-resource
func (a *API) CreateFileResourceRequest(name string, opts ...CreateFileOption) client.APIRequest[*FileUploadCredentials] {
	c := createFileConfig{name: name}
	for _, opt := range opts {
		opt.applyCreateFileOption(&c)
	}

	file := &FileUploadCredentials{}
	request := a.
		newRequest(StorageAPI).
		WithResult(file).
		WithPost("files/prepare").
		WithFormBody(client.ToFormBody(c.toMap())).
		WithOnSuccess(func(ctx context.Context, response client.HTTPResponse) error {
			file.ContentType = c.contentType
			file.FederationToken = true
			file.IsPermanent = c.isPermanent
			file.Notify = c.notify
			return nil
		})
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

// GetFileWithCredentialsRequest https://keboola.docs.apiary.io/#reference/files/manage-files/file-detail
func (a *API) GetFileWithCredentialsRequest(id int) client.APIRequest[*FileDownloadCredentials] {
	file := &FileDownloadCredentials{}
	request := a.
		newRequest(StorageAPI).
		WithResult(file).
		WithGet("files/{fileId}").
		AndPathParam("fileId", strconv.Itoa(id)).
		AndQueryParam("federationToken", "1")
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

func NewSliceURL(file *FileUploadCredentials, slice string) (string, error) {
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

func NewSlicedFileManifest(file *FileUploadCredentials, sliceNames []string) (*SlicedFileManifest, error) {
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
