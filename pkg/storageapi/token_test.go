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
		BucketPermissions{bucket.ID: BucketPermissionRead},
		token.BucketPermissions,
	)
	assert.Equal(t,
		[]string{"keboola.ex-aws-s3"},
		token.ComponentAccess,
	)
}

func TestListAndDeleteToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := ClientForAnEmptyProject(t)

	// Create tokens
	token1, err := CreateTokenRequest(WithDescription("token1"), WithExpiresIn(5*time.Minute)).Send(ctx, c)
	assert.NoError(t, err)
	token2, err := CreateTokenRequest(WithDescription("token2"), WithExpiresIn(5*time.Minute)).Send(ctx, c)
	assert.NoError(t, err)

	// List
	allTokens, err := ListTokensRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, []*Token{token1, token2}, ignoreMasterTokens(*allTokens))

	// Delete token1
	_, err = DeleteTokenRequest(token1.ID).Send(ctx, c)
	assert.NoError(t, err)

	// List
	allTokens, err = ListTokensRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, []*Token{token2}, ignoreMasterTokens(*allTokens))

	// Delete token2
	_, err = DeleteTokenRequest(token2.ID).Send(ctx, c)
	assert.NoError(t, err)

	// List
	allTokens, err = ListTokensRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.Empty(t, ignoreMasterTokens(*allTokens))
}

func TestRefreshToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)

	created, err := CreateTokenRequest(
		WithDescription("refresh token request test"),
		WithExpiresIn(5*time.Minute),
	).Send(ctx, c)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	refreshed, err := RefreshTokenRequest(created.ID).Send(ctx, c)
	assert.NoError(t, err)

	assert.Equal(t, created.Description, refreshed.Description)
	assert.NotEqual(t, refreshed.Created, refreshed.Refreshed)
}

func ignoreMasterTokens(in []*Token) (out []*Token) {
	for _, t := range in {
		if !t.IsMaster {
			out = append(out, t)
		}
	}
	return out
}
