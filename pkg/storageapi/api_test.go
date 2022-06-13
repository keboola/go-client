package storageapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"

	. "github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
)

func clientForRandomProject(t *testing.T) (*testproject.Project, client.Client) {
	project := testproject.GetTestProject(t)
	c := APIClientWithToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	return project, c
}

func clientForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender) {
	project, c := clientForRandomProject(t)
	_, err := CleanProjectRequest().Send(context.Background(), c)
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return project, c
}
