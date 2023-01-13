package keboola_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
)

func TestEncryptRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)
	c := client.NewTestClient()
	api := keboola.NewAPI(project.StorageAPIHost(), keboola.WithClient(&c))

	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	encryptedMapPtr, err := api.EncryptRequest(1234, "keboola.ex-generic-v2", mapToEncrypt).Send(ctx)
	assert.NoError(t, err)
	encryptedMap := *encryptedMapPtr
	assert.NotEmpty(t, encryptedMap)
	assert.True(t, keboola.IsEncrypted(encryptedMap["#keyToEncrypt"]))
}

func TestError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, _ := testproject.GetTestProjectForTest(t)
	c := client.NewTestClient()
	api := keboola.NewAPI(project.StorageAPIHost(), keboola.WithClient(&c))

	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	assert.PanicsWithError(t, "the componentId parameter is required", func() {
		_, _ = api.EncryptRequest(1234, "", mapToEncrypt).Send(ctx)
	})
}
