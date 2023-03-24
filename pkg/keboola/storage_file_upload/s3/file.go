package s3

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

const Provider = "aws"

type Path struct {
	Key    string `json:"key"`
	Bucket string `json:"bucket"`
}

//nolint:tagliatelle
type Credentials struct {
	AccessKeyID     string       `json:"AccessKeyId"`
	SecretAccessKey string       `json:"SecretAccessKey"`
	SessionToken    string       `json:"SessionToken"`
	Expiration      iso8601.Time `json:"Expiration"`
}

//nolint:tagliatelle
type UploadParams struct {
	Path
	Credentials Credentials                  `json:"credentials"`
	ACL         s3types.ObjectCannedACL      `json:"acl"`
	Encryption  s3types.ServerSideEncryption `json:"x-amz-server-side-encryption"`
}

type DownloadParams struct {
	Credentials Credentials `json:"credentials"`
	Path        Path        `json:"s3Path"`
}

func (p *DownloadParams) DestinationURL() (string, error) {
	return fmt.Sprintf("s3://%s/%s", p.Path.Bucket, p.Path.Key), nil
}

func NewUploadWriter(ctx context.Context, params *UploadParams, region string, slice string, transport http.RoundTripper) (*blob.Writer, error) {
	cred := config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			params.Credentials.AccessKeyID,
			params.Credentials.SecretAccessKey,
			params.Credentials.SessionToken,
		),
	)
	var cfg aws.Config
	var err error
	if transport != nil {
		cfg, err = config.LoadDefaultConfig(ctx, cred, config.WithRegion(region), config.WithHTTPClient(&http.Client{Transport: transport}))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, cred, config.WithRegion(region))
	}
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	b, err := s3blob.OpenBucketV2(ctx, client, params.Bucket, nil)
	if err != nil {
		return nil, err
	}

	opts := &blob.WriterOptions{
		BeforeWrite: func(as func(interface{}) bool) error {
			var req *s3.PutObjectInput
			if as(&req) {
				req.ACL = params.ACL
				req.ServerSideEncryption = params.Encryption
			}
			return nil
		},
		// Smaller buffer size for better progress reporting
		// 5MB is AWS's minimum part size, see https://github.com/aws/aws-sdk-go/blob/8cf662a972fa7fba8f2c1ec57648cf840e2bb401/service/s3/s3manager/upload.go#L26-L30
		BufferSize: int(s3manager.MinUploadPartSize),
	}

	bw, err := b.NewWriter(ctx, sliceKey(params.Key, slice), opts)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.Key, err)
	}

	return bw, nil
}

func NewDownloadReader(ctx context.Context, params *DownloadParams, region string, slice string, transport http.RoundTripper) (*blob.Reader, error) {
	b, err := openBucketForDownload(ctx, params, region, transport)
	if err != nil {
		return nil, err
	}

	opts := &blob.ReaderOptions{}
	br, err := b.NewReader(ctx, sliceKey(params.Path.Key, slice), opts)
	if err != nil {
		return nil, fmt.Errorf(`reader: opening blob "%s" failed: %w`, params.Path.Key, err)
	}
	return br, nil
}

func GetFileAttributes(ctx context.Context, params *DownloadParams, region string, slice string, transport http.RoundTripper) (*blob.Attributes, error) {
	b, err := openBucketForDownload(ctx, params, region, transport)
	if err != nil {
		return nil, err
	}

	return b.Attributes(ctx, sliceKey(params.Path.Key, slice))
}

func openBucketForDownload(ctx context.Context, params *DownloadParams, region string, transport http.RoundTripper) (*blob.Bucket, error) {
	cred := config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			params.Credentials.AccessKeyID,
			params.Credentials.SecretAccessKey,
			params.Credentials.SessionToken,
		),
	)

	var cfg aws.Config
	var err error
	if transport != nil {
		cfg, err = config.LoadDefaultConfig(ctx, cred, config.WithRegion(region), config.WithHTTPClient(&http.Client{Transport: transport}))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, cred, config.WithRegion(region))
	}
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	return s3blob.OpenBucketV2(ctx, client, params.Path.Bucket, nil)
}

func NewSliceURL(params *UploadParams, slice string) string {
	return fmt.Sprintf("s3://%s/%s", params.Bucket, sliceKey(params.Key, slice))
}

func sliceKey(key, slice string) string {
	return key + slice
}
