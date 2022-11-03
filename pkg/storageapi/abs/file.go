package abs

import (
	"context"
	"fmt"
	"strings"

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

type ConnectionString struct {
	BlobEndpoint          string
	SharedAccessSignature string
}

func (cs *ConnectionString) ServiceURL() string {
	return fmt.Sprintf("%s?%s", cs.BlobEndpoint, cs.SharedAccessSignature)
}

func parseConnectionString(str string) (*ConnectionString, error) {
	csMap := make(map[string]string)
	for _, item := range strings.Split(str, ";") {
		itemKey := item[:strings.IndexByte(item, '=')]
		itemVal := item[strings.IndexByte(item, '=')+1:]
		csMap[itemKey] = itemVal
	}
	cs := &ConnectionString{}
	val, ok := csMap["BlobEndpoint"]
	if !ok {
		return nil, fmt.Errorf(`connection string is missing "BlobEndpoint" part`)
	}
	cs.BlobEndpoint = val

	val, ok = csMap["SharedAccessSignature"]
	if !ok {
		return nil, fmt.Errorf(`connection string is missing "SharedAccessSignature" part`)
	}
	cs.SharedAccessSignature = val

	return cs, nil
}

func OpenBucket(ctx context.Context, uploadParams UploadParams) (*blob.Bucket, error) {
	cs, err := parseConnectionString(uploadParams.Credentials.SASConnectionString)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewServiceClientWithNoCredential(cs.ServiceURL(), &azblob.ClientOptions{})
	if err != nil {
		return nil, err
	}

	b, err := azureblob.OpenBucket(ctx, client, uploadParams.Container, nil)
	if err != nil {
		return nil, err
	}

	return b, nil
}
