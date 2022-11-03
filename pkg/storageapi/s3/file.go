package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
}

func OpenBucket(ctx context.Context, uploadParams UploadParams, region string) (*blob.Bucket, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
		uploadParams.Credentials.AccessKeyId,
		uploadParams.Credentials.SecretAccessKey,
		uploadParams.Credentials.SessionToken,
	)))
	cfg.Region = region
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	b, err := s3blob.OpenBucketV2(ctx, client, uploadParams.Bucket, nil)
	if err != nil {
		return nil, err
	}

	return b, nil
}
