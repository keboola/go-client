package storageapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func clientForRandomProject(t *testing.T) (*testproject.Project, client.Client) {
	t.Helper()

	project := testproject.GetTestProject(t)
	c := ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	return project, c
}

func clientForAnEmptyProject(t *testing.T) client.Sender {
	t.Helper()

	project, c := clientForRandomProject(t)
	_, err := CleanProjectRequest().Send(context.Background(), c)
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return c
}
