package storageapi

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
)

func ClientForRandomProject(t *testing.T, opts ...testproject.Option) (*testproject.Project, client.Client) {
	t.Helper()

	project, err := testproject.GetTestProjectForTest(t, opts...)
	assert.NoError(t, err)
	c := ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	return project, c
}

func APIClientForAnEmptyProject(t *testing.T, opts ...testproject.Option) *API {
	t.Helper()

	project, c := ClientForRandomProject(t, opts...)
	api := NewAPI(c)
	_, err := api.CleanProjectRequest().Send(context.Background())
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return api
}
