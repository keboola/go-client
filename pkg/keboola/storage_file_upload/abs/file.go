package abs

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

const Provider = "azure"

type ConnectionString struct {
	BlobEndpoint          string
	SharedAccessSignature string
}

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

type DownloadParams struct {
	Credentials Credentials `json:"absCredentials"`
	Path        struct {
		BlobName  string `json:"name"`
		Container string `json:"container"`
	} `json:"absPath"`
}

func (p *DownloadParams) DestinationURL() (string, error) {
	cs, err := parseConnectionString(p.Credentials.SASConnectionString)
	if err != nil {
		return "", err
	}
	blobEndpoint := strings.ReplaceAll(cs.BlobEndpoint, "https://", "azure://")
	return fmt.Sprintf("%s/%s/%s", blobEndpoint, p.Path.Container, p.Path.BlobName), nil
}

func (cs *ConnectionString) ForContainer(container string) string {
	return runtime.JoinPaths(cs.BlobEndpoint, container) + "?" + cs.SharedAccessSignature
}

func NewUploadWriter(ctx context.Context, params *UploadParams, slice string, transport http.RoundTripper) (*blob.Writer, error) {
	cs, err := parseConnectionString(params.Credentials.SASConnectionString)
	if err != nil {
		return nil, err
	}

	clientOptions := &container.ClientOptions{}
	if transport != nil {
		clientOptions.Transport = &http.Client{Transport: transport}
	}
	client, err := container.NewClientWithNoCredential(cs.ForContainer(params.Container), clientOptions)
	if err != nil {
		return nil, err
	}
	b, err := azureblob.OpenBucket(ctx, client, nil)
	if err != nil {
		return nil, err
	}

	// Smaller buffer size for better progress reporting
	opts := &blob.WriterOptions{BufferSize: 512000}
	bw, err := b.NewWriter(ctx, sliceKey(params.BlobName, slice), opts)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.BlobName, err)
	}

	return bw, nil
}

func NewDownloadReader(ctx context.Context, params *DownloadParams, slice string, transport http.RoundTripper) (*blob.Reader, error) {
	b, err := openBucketForDownload(ctx, params, transport)
	if err != nil {
		return nil, err
	}

	br, err := b.NewReader(ctx, sliceKey(params.Path.BlobName, slice), nil)
	if err != nil {
		return nil, fmt.Errorf(`reader: opening blob "%s" failed: %w`, params.Path.BlobName, err)
	}

	return br, nil
}

func GetFileAttributes(ctx context.Context, params *DownloadParams, slice string, transport http.RoundTripper) (*blob.Attributes, error) {
	b, err := openBucketForDownload(ctx, params, transport)
	if err != nil {
		return nil, err
	}

	return b.Attributes(ctx, sliceKey(params.Path.BlobName, slice))
}

func openBucketForDownload(ctx context.Context, params *DownloadParams, transport http.RoundTripper) (*blob.Bucket, error) {
	cs, err := parseConnectionString(params.Credentials.SASConnectionString)
	if err != nil {
		return nil, err
	}

	clientOptions := &container.ClientOptions{}
	if transport != nil {
		clientOptions.Transport = &http.Client{Transport: transport}
	}
	client, err := container.NewClientWithNoCredential(cs.ForContainer(params.Path.Container), clientOptions)
	if err != nil {
		return nil, err
	}

	return azureblob.OpenBucket(ctx, client, nil)
}

func NewSliceURL(params *UploadParams, slice string) string {
	return fmt.Sprintf("azure://%s.blob.core.windows.net/%s/%s", params.AccountName, params.Container, sliceKey(params.BlobName, slice))
}

func sliceKey(key, slice string) string {
	return key + slice
}

func parseConnectionString(str string) (*ConnectionString, error) {
	csMap := make(map[string]string)
	for item := range strings.SplitSeq(str, ";") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf(`connection string is malformed, it should contain key value pairs separated by semicolons`)
		}
		csMap[parts[0]] = parts[1]
	}
	cs := &ConnectionString{}

	if val, ok := csMap["BlobEndpoint"]; ok {
		cs.BlobEndpoint = val
	} else {
		return nil, fmt.Errorf(`connection string is missing "BlobEndpoint" part`)
	}

	if val, ok := csMap["SharedAccessSignature"]; ok {
		cs.SharedAccessSignature = val
	} else {
		return nil, fmt.Errorf(`connection string is missing "SharedAccessSignature" part`)
	}

	return cs, nil
}
