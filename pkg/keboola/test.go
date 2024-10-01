package keboola

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"

	"github.com/keboola/go-client/pkg/client"
)

func APIClientForRandomProject(t *testing.T, ctx context.Context, opts ...testproject.Option) (*testproject.Project, *AuthorizedAPI) {
	t.Helper()

	project, err := testproject.GetTestProjectForTest(t, opts...)
	if err != nil {
		t.Fatal(err)
	}

	c := client.NewTestClient()

	publicAPI, err := NewPublicAPI(ctx, project.StorageAPIHost(), WithClient(&c))
	if err != nil {
		t.Fatal(err)
	}

	api := publicAPI.NewAuthorizedAPI(project.StorageAPIToken(), 1*time.Minute)

	return project, api
}

func APIClientForAnEmptyProject(t *testing.T, ctx context.Context, opts ...testproject.Option) (*testproject.Project, *AuthorizedAPI) {
	t.Helper()

	project, api := APIClientForRandomProject(t, ctx, opts...)
	_, err := api.CleanProjectRequest().Send(ctx)
	if err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}
	return project, api
}
