package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/relvacode/iso8601"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

//nolint:tagliatelle
type UploadParamsCredentials struct {
	AccessKeyId     string       `json:"AccessKeyId"`
	SecretAccessKey string       `json:"SecretAccessKey"`
	SessionToken    string       `json:"SessionToken"`
	Expiration      iso8601.Time `json:"Expiration"`
}

//nolint:tagliatelle
type UploadParams struct {
	Key         string                  `json:"key"`
	Bucket      string                  `json:"bucket"`
	Acl         string                  `json:"acl"`
	Credentials UploadParamsCredentials `json:"credentials"`
	Encryption  string                  `json:"x-amz-server-side-encryption"`
}

func CreateBucketWriter(ctx context.Context, params UploadParams, region string, isEncrypted bool) (*blob.Writer, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
		params.Credentials.AccessKeyId,
		params.Credentials.SecretAccessKey,
		params.Credentials.SessionToken,
	)))
	cfg.Region = region
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
				if isEncrypted {
					req.ServerSideEncryption = types.ServerSideEncryption(params.Encryption)
				}
			}
			return nil
		},
	}
	bw, err := b.NewWriter(ctx, params.Key, opts)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.Key, err)
	}

	return bw, nil
}
