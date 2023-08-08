package keboola_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/go-client/pkg/keboola"
)

func TestBucketApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := keboola.APIClientForAnEmptyProject(t, ctx)

	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucket := &keboola.Bucket{
		BranchID: defBranch.ID,
		BucketID: keboola.BucketID{
			Stage:      keboola.BucketStageIn,
			BucketName: fmt.Sprintf("c-test_%d", rand.Int()),
		},
	}

	// Create
	resCreate, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resCreate)

	// Get bucket - find the bucket
	resGet, err := api.GetBucketRequest(bucket.BranchID, bucket.BucketID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resGet)

	// List - find the bucket
	allBuckets, err := api.ListBucketsRequest(bucket.BranchID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *allBuckets, 1)
	assert.Equal(t, bucket, (*allBuckets)[0])

	// Delete
	_, err = api.DeleteBucketRequest(bucket.BranchID, bucket.BucketID, keboola.WithForce()).Send(ctx)
	assert.NoError(t, err)

	// Get bucket - not found
	_, err = api.GetBucketRequest(bucket.BranchID, bucket.BucketID).Send(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("Bucket %s not found", bucket.BucketID.String()))

	// List - empty
	allBuckets, err = api.ListBucketsRequest(bucket.BranchID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *allBuckets, 0)
}
