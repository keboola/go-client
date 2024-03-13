package keboola

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/request"
)

const (
	featureDisableLegacyBucketPrefix = "disable-legacy-bucket-prefix"
)

type BucketKey struct {
	BranchID BranchID `json:"-"`
	BucketID BucketID `json:"id"`
}

func (v BucketKey) String() string {
	return fmt.Sprintf("%s/%s", v.BranchID.String(), v.BucketID.String())
}

type Bucket struct {
	BucketKey
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
func (a *AuthorizedAPI) GetBucketRequest(k BucketKey) request.APIRequest[*Bucket] {
	result := Bucket{BucketKey: k}
	req := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("branch/{branchId}/buckets/{bucketId}").
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("bucketId", k.BucketID.String())
	return request.NewAPIRequest(&result, req)
}

// ListBucketsRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/list-all-buckets
func (a *AuthorizedAPI) ListBucketsRequest(branchID BranchID, opts ...ListBucketsOption) request.APIRequest[*[]*Bucket] {
	config := listBucketsConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Bucket, 0)
	req := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("branch/{branchId}/buckets").
		AndPathParam("branchId", branchID.String()).
		AndQueryParam("include", config.includeString())

	return request.
		NewAPIRequest(&result, req).
		WithOnSuccess(func(ctx context.Context, result *[]*Bucket) error {
			for _, bucket := range *result {
				bucket.BranchID = branchID
			}
			return nil
		})
}

// CreateBucketRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/create-bucket
func (a *AuthorizedAPI) CreateBucketRequest(bucket *Bucket) request.APIRequest[*Bucket] {
	if bucket.BranchID == 0 {
		return request.NewAPIRequest(bucket, request.NewReqDefinitionError(
			errors.New("bucket.BranchID must be set"),
		))
	}

	params := request.StructToMap(bucket, []string{"description", "displayName"})
	if params["displayName"] == "" {
		delete(params, "displayName")
	}

	// If the featureDisableLegacyBucketPrefix is not enabled,
	// the backend adds "c-" prefix to the bucket name,
	// so we need to trim the prefix before the request.
	bucketName := bucket.BucketID.BucketName
	if !a.index.Features.ToMap().Has(featureDisableLegacyBucketPrefix) {
		bucketName = strings.TrimPrefix(bucketName, "c-")
	}

	params["stage"] = bucket.BucketID.Stage
	params["name"] = bucketName

	req := a.
		newRequest(StorageAPI).
		WithResult(bucket).
		WithPost("branch/{branchId}/buckets").
		AndPathParam("branchId", bucket.BranchID.String()).
		WithFormBody(request.ToFormBody(params)).
		WithOnError(ignoreResourceAlreadyExistsError(func(ctx context.Context) error {
			if result, err := a.GetBucketRequest(bucket.BucketKey).Send(ctx); err == nil {
				*bucket = *result
				return nil
			} else {
				return err
			}
		}))
	return request.NewAPIRequest(bucket, req)
}

// DeleteBucketRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/drop-bucket
func (a *AuthorizedAPI) DeleteBucketRequest(k BucketKey, opts ...DeleteOption) request.APIRequest[request.NoResult] {
	req := a.
		DeleteBucketAsyncRequest(k, opts...).
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
func (a *AuthorizedAPI) DeleteBucketAsyncRequest(k BucketKey, opts ...DeleteOption) request.APIRequest[*StorageJob] {
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
		WithDelete("branch/{branchId}/buckets/{bucketId}").
		AndPathParam("branchId", k.BranchID.String()).
		AndPathParam("bucketId", k.BucketID.String()).
		AndQueryParam("async", "1")

	if c.force {
		req = req.AndQueryParam("force", "true")
	}

	return request.NewAPIRequest(result, req)
}
