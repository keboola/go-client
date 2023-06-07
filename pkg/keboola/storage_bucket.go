package keboola

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/request"
)

const (
	featureDisableLegacyBucketPrefix = "disable-legacy-bucket-prefix"
)

type Bucket struct {
	ID             BucketID      `json:"id"`
	URI            string        `json:"uri"`
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
func (a *API) GetBucketRequest(bucketID BucketID) request.APIRequest[*Bucket] {
	result := Bucket{ID: bucketID}
	req := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("buckets/{bucketId}").
		AndPathParam("bucketId", bucketID.String())
	return request.NewAPIRequest(&result, req)
}

// ListBucketsRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/list-all-buckets
func (a *API) ListBucketsRequest(opts ...ListBucketsOption) request.APIRequest[*[]*Bucket] {
	config := listBucketsConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Bucket, 0)
	req := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("buckets").
		AndQueryParam("include", config.includeString())

	return request.NewAPIRequest(&result, req)
}

// CreateBucketRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/create-bucket
func (a *API) CreateBucketRequest(bucket *Bucket) request.APIRequest[*Bucket] {
	// Create config
	params := request.StructToMap(bucket, []string{"description", "displayName"})
	if params["displayName"] == "" {
		delete(params, "displayName")
	}

	// If the featureDisableLegacyBucketPrefix is not enabled,
	// the backend adds "c-" prefix to the bucket name,
	// so we need to trim the prefix before the request.
	bucketName := bucket.ID.BucketName
	if !a.index.Features.ToMap().Has(featureDisableLegacyBucketPrefix) {
		bucketName = strings.TrimPrefix(bucketName, "c-")
	}

	params["stage"] = bucket.ID.Stage
	params["name"] = bucketName

	req := a.
		newRequest(StorageAPI).
		WithResult(bucket).
		WithPost("buckets").
		WithFormBody(request.ToFormBody(params)).
		WithOnError(ignoreResourceAlreadyExistsError(func(ctx context.Context) error {
			if result, err := a.GetBucketRequest(bucket.ID).Send(ctx); err == nil {
				*bucket = *result
				return nil
			} else {
				return err
			}
		}))
	return request.NewAPIRequest(bucket, req)
}

// DeleteBucketRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/drop-bucket
func (a *API) DeleteBucketRequest(bucketID BucketID, opts ...DeleteOption) request.APIRequest[request.NoResult] {
	req := a.
		DeleteBucketAsyncRequest(bucketID, opts...).
		WithOnSuccess(func(ctx context.Context, job *StorageJob) error {
			// Wait for storage job
			waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*1)
			defer waitCancelFn()
			if err := a.WaitForStorageJob(waitCtx, job); err != nil {
				return err
			}
			return nil
		})
	return request.NewAPIRequest(request.NoResult{}, req)
}

// DeleteBucketAsyncRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/drop-bucket
func (a *API) DeleteBucketAsyncRequest(bucketID BucketID, opts ...DeleteOption) request.APIRequest[*StorageJob] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	result := &StorageJob{}
	req := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithDelete("buckets/{bucketId}").
		AndPathParam("bucketId", bucketID.String()).
		AndQueryParam("async", "1")

	if c.force {
		req = req.AndQueryParam("force", "true")
	}

	return request.NewAPIRequest(result, req)
}
