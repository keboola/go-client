package storageapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func clientForRandomProject(t *testing.T, opts ...testproject.Option) (*testproject.Project, client.Client) {
	t.Helper()

	project, err := testproject.GetTestProjectForTest(t, opts...)
	assert.NoError(t, err)
	c := ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())
	return project, c
}

func clientForAnEmptyProject(t *testing.T, opts ...testproject.Option) client.Sender {
	t.Helper()

	project, c := clientForRandomProject(t, opts...)
	_, err := CleanProjectRequest().Send(context.Background(), c)
	if err != nil {
		t.Fatalf(`cannot clear project "%d": %s`, project.ID(), err)
	}
	return c
}
