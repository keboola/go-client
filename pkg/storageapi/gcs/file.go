package gcs

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
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

type uploadConfig struct {
	transport http.RoundTripper
}

type UploadOptions func(c *uploadConfig)

func WithUploadTransport(transport http.RoundTripper) UploadOptions {
	return func(c *uploadConfig) {
		c.transport = transport
	}
}

func NewUploadWriter(ctx context.Context, params *UploadParams, slice string, transport http.RoundTripper) (*blob.Writer, error) {
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: params.AccessToken,
		TokenType:   params.TokenType,
	})

	if transport == nil {
		transport = gcp.DefaultTransport()
	}
	client, err := gcp.NewHTTPClient(transport, tokenSource)
	if err != nil {
		return nil, err
	}
	b, err := gcsblob.OpenBucket(ctx, client, params.Bucket, nil)
	if err != nil {
		return nil, err
	}

	var gcsClient *storage.Client
	if b.As(&gcsClient) {
		gcsClient.SetRetry(
			storage.WithBackoff(gax.Backoff{}),
			storage.WithPolicy(storage.RetryIdempotent),
		)
	} else {
		panic("Unable to access storage.Client through Bucket.As")
	}

	bw, err := b.NewWriter(ctx, sliceKey(params.Key, slice), nil)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.Key, err)
	}

	return bw, nil
}

func NewDownloadReader(ctx context.Context, params *UploadParams, slice string) (*blob.Reader, error) {
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

	var gcsClient *storage.Client
	if b.As(&gcsClient) {
		gcsClient.SetRetry(
			storage.WithBackoff(gax.Backoff{}),
			storage.WithPolicy(storage.RetryIdempotent),
		)
	} else {
		panic("Unable to access storage.Client through Bucket.As")
	}

	br, err := b.NewReader(ctx, sliceKey(params.Key, slice), nil)
	if err != nil {
		return nil, fmt.Errorf(`opening blob "%s" failed: %w`, params.Key, err)
	}

	return br, nil
}

func NewSliceUrl(params *UploadParams, slice string) string {
	return fmt.Sprintf(
		"gs://%s/%s",
		params.Bucket,
		sliceKey(params.Key, slice),
	)
}

func sliceKey(key, slice string) string {
	return key + slice
}
