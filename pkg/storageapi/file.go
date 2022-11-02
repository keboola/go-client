package storageapi

import (
	"compress/gzip"
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

func gzReader(r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		_, err := io.Copy(gzip.NewWriter(pw), r)
		if err != nil {
			panic(err)
		}
	}()
	return pr
}

func Upload(ctx context.Context, bucket *blob.Bucket, key string, reader io.Reader) error {
	reader = gzReader(reader)

	bw, err := bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf(`opening blob "%s" failed: %w`, key, err)
	}
	defer bw.Close()

	_, err = io.Copy(bw, reader)
	if err != nil {
		return fmt.Errorf(`writing blob "%s" failed: %w`, key, err)
	}

	return nil
}
