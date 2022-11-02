package storageapi

import (
	"sort"
	"strings"

	"github.com/relvacode/iso8601"

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
	ID             BucketID      `json:"id"`
	Uri            string        `json:"uri"`
	Name           string        `json:"name"`
	DisplayName    string        `json:"displayName"`
	Stage          string        `json:"stage"`
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

// ListBucketsRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/list-all-buckets
func ListBucketsRequest(opts ...ListBucketsOption) client.APIRequest[*[]*Bucket] {
	config := listBucketsConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Bucket, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("buckets").
		AndQueryParam("include", config.includeString())

	return client.NewAPIRequest(&result, request)
}

// CreateBucketRequest https://keboola.docs.apiary.io/#reference/buckets/create-or-list-buckets/create-bucket
func CreateBucketRequest(bucket *Bucket) client.APIRequest[*Bucket] {
	// Create config
	params := client.StructToMap(bucket, []string{"name", "stage", "description", "displayName"})
	if params["displayName"] == "" {
		delete(params, "displayName")
	}
	request := newRequest().
		WithResult(bucket).
		WithPost("buckets").
		WithFormBody(client.ToFormBody(params))
	return client.NewAPIRequest(bucket, request)
}

// DeleteBucketRequest https://keboola.docs.apiary.io/#reference/buckets/manage-bucket/drop-bucket
func DeleteBucketRequest(bucketID BucketID, opts ...DeleteOption) client.APIRequest[client.NoResult] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	request := newRequest().
		WithDelete("buckets/{bucketId}").
		AndPathParam("bucketId", string(bucketID))
	if c.force {
		request = request.AndQueryParam("force", "true")
	}
	return client.NewAPIRequest(client.NoResult{}, request)
}
