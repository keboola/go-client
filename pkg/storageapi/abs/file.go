package abs

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

//nolint:tagliatelle
type UploadParamsCredentials struct {
	SASConnectionString string       `json:"SASConnectionString"`
	Expiration          iso8601.Time `json:"expiration"`
}

type UploadParams struct {
	BlobName    string                  `json:"blobName"`
	AccountName string                  `json:"accountName"`
	Container   string                  `json:"container"`
	Credentials UploadParamsCredentials `json:"absCredentials"`
}

func OpenBucket(ctx context.Context, uploadParams UploadParams, url string) (*blob.Bucket, error) {
	client, err := azblob.NewServiceClientWithNoCredential(url, &azblob.ClientOptions{})
	if err != nil {
		return nil, err
	}

	b, err := azureblob.OpenBucket(ctx, client, uploadParams.Container, nil)
	if err != nil {
		return nil, err
	}

	return b, nil
}
