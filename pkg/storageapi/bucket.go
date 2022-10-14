package storageapi

import (
	"context"
	"sort"
	"strings"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

type Bucket struct {
	ID             BucketID      `json:"id"`
	Uri            string        `json:"uri"`
	DisplayName    string        `json:"displayName"`
	Description    string        `json:"description"`
	Created        iso8601.Time  `json:"created"`
	LastChangeDate *iso8601.Time `json:"lastChangeDate"`
	IsReadOnly     bool          `json:"isReadOnly"`
	DataSizeBytes  uint64        `json:"dataSizeBytes"`
	RowsCount      uint64        `json:"rowsCount"`
}

type listBucketsConfig struct {
	include map[string]bool
}

func (v listBucketsConfig) includeString() string {
	include := make([]string, 0, len(v.include))
	for k := range v.include {
		include = append(include, k)
	}
	sort.Strings(include)
	return strings.Join(include, ",")
}

type ListBucketsOption func(c *listBucketsConfig)

// GetBucketRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/bucket-detail
func (a *Api) GetBucketRequest(bucketID BucketID) client.APIRequest[*Bucket] {
	result := Bucket{ID: bucketID}
	request := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("buckets/{bucketId}").
		AndPathParam("bucketId", bucketID.String())
	return client.NewAPIRequest(&result, request)
}

// ListBucketsRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/list-all-buckets
func (a *Api) ListBucketsRequest(opts ...ListBucketsOption) client.APIRequest[*[]*Bucket] {
	config := listBucketsConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Bucket, 0)
	request := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("buckets").
		AndQueryParam("include", config.includeString())

	return client.NewAPIRequest(&result, request)
}

// CreateBucketRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/create-bucket
func (a *Api) CreateBucketRequest(bucket *Bucket) client.APIRequest[*Bucket] {
	// Create config
	params := client.StructToMap(bucket, []string{"description", "displayName"})
	if params["displayName"] == "" {
		delete(params, "displayName")
	}

	params["stage"] = bucket.ID.Stage
	params["name"] = bucket.ID.BucketName

	request := a.
		newRequest(StorageAPI).
		WithResult(bucket).
		WithPost("buckets").
		WithFormBody(client.ToFormBody(params)).
		WithOnError(ignoreResourceAlreadyExistsError(func(ctx context.Context, sender client.Sender) error {
			if result, err := a.GetBucketRequest(bucket.ID).Send(ctx); err == nil {
				*bucket = *result
				return nil
			} else {
				return err
			}
		}))
	return client.NewAPIRequest(bucket, request)
}

// DeleteBucketRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/drop-bucket
func (a *Api) DeleteBucketRequest(bucketID BucketID, opts ...DeleteOption) client.APIRequest[client.NoResult] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	request := a.
		newRequest(StorageAPI).
		WithDelete("buckets/{bucketId}").
		AndPathParam("bucketId", bucketID.String()).
		WithOnError(ignoreResourceNotFoundError())

	if c.force {
		request = request.AndQueryParam("force", "true")
	}

	return client.NewAPIRequest(client.NoResult{}, request)
}
