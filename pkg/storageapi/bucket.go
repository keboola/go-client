package storageapi

import (
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

type BucketID string

func (v BucketID) String() string {
	return string(v)
}

const (
	BucketStageIn  = "in"
	BucketStageOut = "out"
)

type Bucket struct {
	ID             BucketID `json:"id"`
	Uri            string   `json:"uri"`
	Name           string   `json:"name"`
	DisplayName    string   `json:"displayName"`
	Stage          string   `json:"stage"`
	Description    string   `json:"description"`
	Created        Time     `json:"created"`
	LastChangeDate *Time    `json:"lastChangeDate"`
	IsReadOnly     bool     `json:"isReadOnly"`
	DataSizeBytes  uint64   `json:"dataSizeBytes"`
	RowsCount      uint64   `json:"rowsCount"`
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
	params := client.StructToMap(bucket, []string{"name", "stage", "description", "displayName"})
	if params["displayName"] == "" {
		delete(params, "displayName")
	}
	request := a.
		newRequest(StorageAPI).
		WithResult(bucket).
		WithPost("buckets").
		WithFormBody(client.ToFormBody(params))
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
		AndPathParam("bucketId", string(bucketID))
	if c.force {
		request = request.AndQueryParam("force", "true")
	}
	return client.NewAPIRequest(client.NoResult{}, request)
}
