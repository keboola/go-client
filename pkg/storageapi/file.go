package storageapi

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi/abs"
	"github.com/keboola/go-client/pkg/storageapi/s3"
)

type File struct {
	ID              int              `json:"id" readonly:"true"`
	Created         iso8601.Time     `json:"created" readonly:"true"`
	IsPublic        bool             `json:"isPublic,omitempty"`
	IsSliced        bool             `json:"sliced,omitempty"`
	IsEncrypted     bool             `json:"isEncrypted,omitempty"`
	Name            string           `json:"name"`
	Url             string           `json:"url" readonly:"true"`
	Provider        string           `json:"provider" readonly:"true"`
	Region          string           `json:"region" readonly:"true"`
	SizeBytes       int              `json:"sizeBytes,omitempty"`
	Tags            []string         `json:"tags,omitempty"`
	MaxAgeDays      int              `json:"maxAgeDays" readonly:"true"`
	S3UploadParams  s3.UploadParams  `json:"uploadParams,omitempty" readonly:"true"`
	ABSUploadParams abs.UploadParams `json:"absUploadParams,omitempty" readonly:"true"`

	ContentType     string `json:"contentType,omitempty"`
	FederationToken bool   `json:"federationToken,omitempty"`
	IsPermanent     bool   `json:"isPermanent,omitempty"`
	Notify          bool   `json:"notify,omitempty"`
}

// CreateFileResourceRequest https://keboola.docs.apiary.io/#reference/files/upload-file/create-file-resource
func CreateFileResourceRequest(file *File) client.APIRequest[*File] {
	file.FederationToken = true
	request := newRequest().
		WithResult(file).
		WithPost("files/prepare").
		WithFormBody(client.ToFormBody(client.StructToMap(file, nil)))
	return client.NewAPIRequest(file, request)
}

// GetFileResourceRequest https://keboola.docs.apiary.io/#reference/files/manage-files/file-detail
func GetFileResourceRequest(id int) client.APIRequest[*File] {
	file := &File{}
	request := newRequest().
		WithResult(file).
		WithGet("files/{fileId}").
		AndPathParam("fileId", strconv.Itoa(id))
	return client.NewAPIRequest(file, request)
}

// NewUploadWriter instantiates a Writer to the Storage given by cloud provider specified in the File resource.
func NewUploadWriter(ctx context.Context, file *File) (*blob.Writer, error) {
	switch file.Provider {
	case abs.Provider:
		return abs.NewUploadWriter(ctx, file.ABSUploadParams)
	case s3.Provider:
		return s3.NewUploadWriter(ctx, file.S3UploadParams, file.Region)
	default:
		return nil, fmt.Errorf(`unsupported provider "%s"`, file.Provider)
	}
}

// Upload instantiates a Writer to the Storage given by cloud provider specified in the File resource and writes there
// content of the reader.
func Upload(ctx context.Context, file *File, fr io.Reader) (written int64, err error) {
	bw, err := NewUploadWriter(ctx, file)
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
