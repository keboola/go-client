package keboola_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/keboola"
)

func TestVerifyToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, c := ClientForRandomProject(t)
	api := NewAPI(c)

	token, err := api.VerifyTokenRequest(project.StorageAPIToken()).Send(ctx)
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
	api := NewAPI(c)

	token, err := api.VerifyTokenRequest("").Send(ctx)
	assert.Error(t, err)
	apiErr := err.(*StorageError)
	assert.Equal(t, "Access token must be set", apiErr.Message)
	assert.Equal(t, "", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.StatusCode())
	assert.Empty(t, token)
}

func TestVerifyTokenInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)
	api := NewAPI(c)

	token, err := api.VerifyTokenRequest("mytoken").Send(ctx)
	assert.Error(t, err)
	apiErr := err.(*StorageError)
	assert.Equal(t, "Invalid access token", apiErr.Message)
	assert.Equal(t, "storage.tokenInvalid", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.StatusCode())
	assert.Empty(t, token)
}

func TestCreateToken_AllPerms(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)
	api := NewAPI(c)

	description := "create token request all perms test"
	token, err := api.CreateTokenRequest(
		WithDescription(description),
		WithCanReadAllFileUploads(true),
		WithCanPurgeTrash(true),
		WithCanManageBuckets(true),
		WithExpiresIn(5*time.Minute),
	).Send(ctx)
	assert.NoError(t, err)

	assert.Equal(t, description, token.Description)
	assert.True(t, token.CanManageBuckets && token.CanPurgeTrash && token.CanReadAllFileUploads)
	assert.Empty(t, token.ComponentAccess)
}

func TestCreateToken_SomePerms(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)
	api := NewAPI(c)

	rand.Seed(time.Now().Unix())

	bucket, err := api.CreateBucketRequest(&Bucket{
		ID: BucketID{
			Stage:      BucketStageIn,
			BucketName: fmt.Sprintf("create_token_test_%d", rand.Int()),
		},
	}).Send(ctx)
	assert.NoError(t, err)

	description := "create token request all perms test"
	token, err := api.CreateTokenRequest(
		WithDescription(description),
		WithBucketPermission(bucket.ID, BucketPermissionRead),
		WithComponentAccess("keboola.ex-aws-s3"),
		WithExpiresIn(5*time.Minute),
	).Send(ctx)
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
	api := APIClientForAnEmptyProject(t)

	// Create tokens
	token1, err := api.CreateTokenRequest(WithDescription("token1"), WithExpiresIn(5*time.Minute)).Send(ctx)
	assert.NoError(t, err)
	token2, err := api.CreateTokenRequest(WithDescription("token2"), WithExpiresIn(5*time.Minute)).Send(ctx)
	assert.NoError(t, err)

	// List
	allTokens, err := api.ListTokensRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []*Token{token1, token2}, ignoreMasterTokens(*allTokens))

	// Delete token1
	_, err = api.DeleteTokenRequest(token1.ID).Send(ctx)
	assert.NoError(t, err)

	// List
	allTokens, err = api.ListTokensRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []*Token{token2}, ignoreMasterTokens(*allTokens))

	// Delete token2
	_, err = api.DeleteTokenRequest(token2.ID).Send(ctx)
	assert.NoError(t, err)

	// List
	allTokens, err = api.ListTokensRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Empty(t, ignoreMasterTokens(*allTokens))
}

func TestRefreshToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := ClientForRandomProject(t)
	api := NewAPI(c)

	created, err := api.CreateTokenRequest(
		WithDescription("refresh token request test"),
		WithExpiresIn(5*time.Minute),
	).Send(ctx)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	refreshed, err := api.RefreshTokenRequest(created.ID).Send(ctx)
	assert.NoError(t, err)

	assert.Equal(t, created.Description, refreshed.Description)
	assert.NotEqual(t, refreshed.Created, refreshed.Refreshed)
}

func TestToken_JSON(t *testing.T) {
	t.Parallel()

	token := &Token{
		Token:       "secret",
		ID:          "1234",
		Description: "description",
		BucketPermissions: BucketPermissions{
			MustParseBucketID("in.c-bucket"): BucketPermissionRead,
		},
	}

	bytes, err := json.Marshal(token)
	assert.NoError(t, err)

	var decoded *Token
	err = json.Unmarshal(bytes, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, token, decoded)
}

func ignoreMasterTokens(in []*Token) (out []*Token) {
	for _, t := range in {
		if !t.IsMaster {
			out = append(out, t)
		}
	}
	return out
}
