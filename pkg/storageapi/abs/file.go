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

const Provider = "azure"

//nolint:tagliatelle
type Credentials struct {
	SASConnectionString string       `json:"SASConnectionString"`
	Expiration          iso8601.Time `json:"expiration"`
}

type UploadParams struct {
	BlobName    string      `json:"blobName"`
	AccountName string      `json:"accountName"`
	Container   string      `json:"container"`
	Credentials Credentials `json:"absCredentials"`
}

type ConnectionString struct {
	BlobEndpoint          string
	SharedAccessSignature string
}

func (cs *ConnectionString) ServiceURL() string {
	return fmt.Sprintf("%s?%s", cs.BlobEndpoint, cs.SharedAccessSignature)
}

func NewUploadWriter(ctx context.Context, params UploadParams, slice string) (*blob.Writer, error) {
	cs, err := parseConnectionString(params.Credentials.SASConnectionString)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewServiceClientWithNoCredential(cs.ServiceURL(), &azblob.ClientOptions{})
	if err != nil {
		return nil, err
	}

	b, err := azureblob.OpenBucket(ctx, client, params.Container, nil)
	if err != nil {
		return nil, err
	}

	bw, err := b.NewWriter(ctx, sliceKey(params.BlobName, slice), nil)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.BlobName, err)
	}

	return bw, nil
}

func NewSliceUrl(params UploadParams, slice string) string {
	return fmt.Sprintf("azure://%s.blob.core.windows.net/%s/%s", params.AccountName, params.Container, sliceKey(params.BlobName, slice))
}

func sliceKey(key, slice string) string {
	return key + slice
}

func parseConnectionString(str string) (*ConnectionString, error) {
	csMap := make(map[string]string)
	for _, item := range strings.Split(str, ";") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf(`connection string is malformed, it should contain key value pairs separated by semicolons`)
		}
		csMap[parts[0]] = parts[1]
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
