package storageapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestVerifyToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, c := clientForRandomProject(t)

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
	_, c := clientForRandomProject(t)

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
	_, c := clientForRandomProject(t)

	token, err := VerifyTokenRequest("mytoken").Send(ctx, c)
	assert.Error(t, err)
	apiErr := err.(*Error)
	assert.Equal(t, "Invalid access token", apiErr.Message)
	assert.Equal(t, "storage.tokenInvalid", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.StatusCode())
	assert.Empty(t, token)
}
