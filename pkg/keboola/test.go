package keboola

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"

	"github.com/keboola/go-client/pkg/client"
)

func APIClientForRandomProject(t *testing.T, ctx context.Context, opts ...testproject.Option) (*testproject.Project, *API) {
	t.Helper()

	project, err := testproject.GetTestProjectForTest(t, opts...)
	if err != nil {
		t.Fatal(err)
	}

	c := client.NewTestClient()
	api, err := NewAPI(ctx, project.StorageAPIHost(), WithToken(project.StorageAPIToken()), WithClient(&c))
	if err != nil {
		t.Fatal(err)
	}

	return project, api
}

func APIClientForAnEmptyProject(t *testing.T, ctx context.Context, opts ...testproject.Option) *API {
	t.Helper()

	project, api := APIClientForRandomProject(t, ctx, opts...)
	_, err := api.CleanProjectRequest().Send(ctx)
	if err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}
	return api
}
