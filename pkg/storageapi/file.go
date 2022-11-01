package storageapi

import (
	"github.com/keboola/go-client/pkg/client"
)

//nolint:tagliatelle
type S3UploadParamsCredentials struct {
	AccessKeyId     string      `json:"AccessKeyId"`
	SecretAccessKey string      `json:"SecretAccessKey"`
	SessionToken    string      `json:"SessionToken"`
	Expiration      client.Time `json:"Expiration"`
}

type S3UploadParams struct {
	Key         string                    `json:"key"`
	Bucket      string                    `json:"bucket"`
	Acl         string                    `json:"acl"`
	Credentials S3UploadParamsCredentials `json:"credentials"`
}

//nolint:tagliatelle
type ABSUploadParamsCredentials struct {
	SASConnectionString string `json:"SASConnectionString"`
	Expiration          Time   `json:"expiration"`
}

type ABSUploadParams struct {
	BlobName    string                     `json:"blobName"`
	AccountName string                     `json:"accountName"`
	Container   string                     `json:"container"`
	Credentials ABSUploadParamsCredentials `json:"absCredentials"`
}

type File struct {
	ID              int             `json:"id" readonly:"true"`
	Created         Time            `json:"created" readonly:"true"`
	IsPublic        bool            `json:"isPublic,omitempty"`
	IsSliced        bool            `json:"sliced,omitempty"`
	IsEncrypted     bool            `json:"isEncrypted,omitempty"`
	Name            string          `json:"name"`
	Url             string          `json:"url" readonly:"true"`
	Provider        string          `json:"provider" readonly:"true"`
	Region          string          `json:"region" readonly:"true"`
	SizeBytes       int             `json:"sizeBytes,omitempty"`
	Tags            []string        `json:"tags,omitempty"`
	MaxAgeDays      int             `json:"maxAgeDays" readonly:"true"`
	UploadParams    S3UploadParams  `json:"uploadParams,omitempty" readonly:"true"`
	AbsUploadParams ABSUploadParams `json:"absUploadParams,omitempty" readonly:"true"`

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
