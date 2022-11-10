package gcs

import (
	"context"
	"fmt"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
)

const Provider = "gcp"

//nolint:tagliatelle
type UploadParams struct {
	ProjectId   string `json:"projectId"`
	Key         string `json:"key"`
	Bucket      string `json:"bucket"`
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

func NewUploadWriter(ctx context.Context, params UploadParams, slice string) (*blob.Writer, error) {
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: params.AccessToken,
		TokenType:   params.TokenType,
	})

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), tokenSource)
	if err != nil {
		return nil, err
	}
	b, err := gcsblob.OpenBucket(ctx, client, params.Bucket, nil)
	if err != nil {
		return nil, err
	}

	bw, err := b.NewWriter(ctx, sliceKey(params.Key, slice), nil)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.Key, err)
	}

	return bw, nil
}

func NewSliceUrl(params UploadParams, slice string) string {
	return fmt.Sprintf(
		"gs://%s/%s",
		params.Bucket,
		sliceKey(params.Key, slice),
	)
}

func sliceKey(key, slice string) string {
	return key + slice
}
