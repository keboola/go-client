package storageapi_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestVerifyToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, c := ClientForRandomProject(t)

	token, err := VerifyTokenRequest(project.StorageAPIToken()).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, project.ID(), token.ProjectID())
	assert.NotEmpty(t, token.ProjectName())
	assert.NotEmpty(t, token.Owner.Features)
	if token.IsMaster {
		assert.NotNil(t, token.Admin)
		assert.NotEmpty(t, token.Description)
	} else {
		assert.Nil(t, token.Admin)
	}
}

func TestVerifyTokenEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)

	token, err := VerifyTokenRequest("").Send(ctx, c)
	assert.Error(t, err)
	apiErr := err.(*Error)
	assert.Equal(t, "Access token must be set", apiErr.Message)
	assert.Equal(t, "", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.StatusCode())
	assert.Empty(t, token)
}

func TestVerifyTokenInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)

	token, err := VerifyTokenRequest("mytoken").Send(ctx, c)
	assert.Error(t, err)
	apiErr := err.(*Error)
	assert.Equal(t, "Invalid access token", apiErr.Message)
	assert.Equal(t, "storage.tokenInvalid", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.StatusCode())
	assert.Empty(t, token)
}

func TestCreateToken_AllPerms(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)

	description := "create token request all perms test"
	token, err := CreateTokenRequest(
		WithDescription(description),
		WithCanReadAllFileUploads(true),
		WithCanPurgeTrash(true),
		WithCanManageBuckets(true),
		WithExpiresIn(5*time.Minute),
	).Send(ctx, c)
	assert.NoError(t, err)

	assert.Equal(t, description, token.Description)
	assert.True(t, token.CanManageBuckets && token.CanPurgeTrash && token.CanReadAllFileUploads)
	assert.Empty(t, token.ComponentAccess)
}

func TestCreateToken_SomePerms(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)

	rand.Seed(time.Now().Unix())

	bucket, err := CreateBucketRequest(&Bucket{
		Name:  fmt.Sprintf("create_token_test_%d", rand.Int()),
		Stage: BucketStageIn,
	}).Send(ctx, c)
	assert.NoError(t, err)

	description := "create token request all perms test"
	token, err := CreateTokenRequest(
		WithDescription(description),
		WithBucketPermission(bucket.ID, BucketPermissionRead),
		WithComponentAccess("keboola.ex-aws-s3"),
		WithExpiresIn(5*time.Minute),
	).Send(ctx, c)
	assert.NoError(t, err)

	assert.Equal(t, description, token.Description)
	assert.Equal(t,
		map[BucketID]BucketPermission{bucket.ID: BucketPermissionRead},
		token.BucketPermissions,
	)
	assert.Equal(t,
		[]string{"keboola.ex-aws-s3"},
		token.ComponentAccess,
	)
}
