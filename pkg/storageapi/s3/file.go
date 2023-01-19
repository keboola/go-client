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
	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

const Provider = "aws"

//nolint:tagliatelle
type Credentials struct {
	AccessKeyId     string       `json:"AccessKeyId"`
	SecretAccessKey string       `json:"SecretAccessKey"`
	SessionToken    string       `json:"SessionToken"`
	Expiration      iso8601.Time `json:"Expiration"`
}

//nolint:tagliatelle
type UploadParams struct {
	Key         string                       `json:"key"`
	Bucket      string                       `json:"bucket"`
	Credentials Credentials                  `json:"credentials"`
	Acl         s3types.ObjectCannedACL      `json:"acl"`
	Encryption  s3types.ServerSideEncryption `json:"x-amz-server-side-encryption"`
}

func NewUploadWriter(ctx context.Context, params *UploadParams, region string, slice string, transport http.RoundTripper) (*blob.Writer, error) {
	cred := config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			params.Credentials.AccessKeyId,
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
				req.ACL = params.Acl
				req.ServerSideEncryption = params.Encryption
			}
			return nil
		},
	}

	bw, err := b.NewWriter(ctx, sliceKey(params.Key, slice), opts)
	if err != nil {
		return nil, fmt.Errorf(`writer: opening blob "%s" failed: %w`, params.Key, err)
	}

	return bw, nil
}

func NewDownloadReader(ctx context.Context, params *UploadParams, region string, slice string) (*blob.Reader, error) {
	cred := config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			params.Credentials.AccessKeyId,
			params.Credentials.SecretAccessKey,
			params.Credentials.SessionToken,
		),
	)

	cfg, err := config.LoadDefaultConfig(ctx, cred, config.WithRegion(region))
	client := s3.NewFromConfig(cfg)
	b, err := s3blob.OpenBucketV2(ctx, client, params.Bucket, nil)
	if err != nil {
		return nil, err
	}

	opts := &blob.ReaderOptions{}
	br, err := b.NewReader(ctx, sliceKey(params.Key, slice), opts)
	if err != nil {
		return nil, fmt.Errorf(`reader: opening blob "%s" failed: %w`, params.Key, err)
	}
	return br, nil
}

func NewSliceUrl(params *UploadParams, slice string) string {
	return fmt.Sprintf("s3://%s/%s", params.Bucket, sliceKey(params.Key, slice))
}

func sliceKey(key, slice string) string {
	return key + slice
}
