package storageapi_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"
)

func TestBucketApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForAnEmptyProject(t)

	bucketName := fmt.Sprintf("test_%d", rand.Int())

	// Delete the bucket if it exists beforehand
	_, _ = storageapi.DeleteBucketRequest(storageapi.BucketID(fmt.Sprintf("in.c-%s", bucketName)), storageapi.WithForce()).Send(ctx, c)

	bucket := &storageapi.Bucket{
		Name:  bucketName,
		Stage: "in",
	}

	// Create
	resCreate, err := storageapi.CreateBucketRequest(bucket).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resCreate)

	// List - find the bucket
	allBuckets, err := storageapi.ListBucketsRequest().Send(ctx, c)
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
	_, err = storageapi.DeleteBucketRequest(bucket.ID, storageapi.WithForce()).Send(ctx, c)
	assert.NoError(t, err)

	// List - don't find the bucket
	allBuckets, err = storageapi.ListBucketsRequest().Send(ctx, c)
	assert.NoError(t, err)
	for _, b := range *allBuckets {
		assert.NotEqual(t, bucket, b)
	}
	assert.True(t, bucketFound)
}
