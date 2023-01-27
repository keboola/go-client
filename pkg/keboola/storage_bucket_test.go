package keboola_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
)

func TestBucketApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	api := keboola.APIClientForAnEmptyProject(t, ctx)

	bucket := &keboola.Bucket{
		ID: keboola.BucketID{
			Stage:      keboola.BucketStageIn,
			BucketName: fmt.Sprintf("c-test_%d", rand.Int()),
		},
	}

	// Create
	resCreate, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resCreate)

	// Get bucket - find the bucket
	resGet, err := api.GetBucketRequest(bucket.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resGet)

	// List - find the bucket
	allBuckets, err := api.ListBucketsRequest().Send(ctx)
	assert.NoError(t, err)
	bucketFound := false
	for _, b := range *allBuckets {
		if b.ID == bucket.ID {
			assert.Equal(t, bucket, b)
			bucketFound = true
		}
	}
	assert.True(t, bucketFound)

	// Delete
	_, err = api.DeleteBucketRequest(bucket.ID, keboola.WithForce()).Send(ctx)
	assert.NoError(t, err)

	// Get bucket - don't find the bucket
	_, err = api.GetBucketRequest(bucket.ID).Send(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("Bucket %s not found", bucket.ID.String()))

	// List - don't find the bucket
	allBuckets, err = api.ListBucketsRequest().Send(ctx)
	assert.NoError(t, err)
	for _, b := range *allBuckets {
		assert.NotEqual(t, bucket, b)
	}
	assert.True(t, bucketFound)
}
