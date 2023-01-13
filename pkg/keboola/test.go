package keboola

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func APIClientForRandomProject(t *testing.T, opts ...testproject.Option) (*testproject.Project, *API) {
	t.Helper()

	project, err := testproject.GetTestProjectForTest(t, opts...)
	assert.NoError(t, err)
	c := client.NewTestClient()
	api := NewAPI(project.StorageAPIHost(), WithToken(project.StorageAPIToken()), WithClient(&c))
	return project, api
}

func APIClientForAnEmptyProject(t *testing.T, opts ...testproject.Option) *API {
	t.Helper()

	project, api := APIClientForRandomProject(t, opts...)
	_, err := api.CleanProjectRequest().Send(context.Background())
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return api
}
