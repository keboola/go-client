package encryptionapi_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/stretchr/testify/assert"
)

func TestEncryptRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := encryptionapi.APIClient(client.NewTestClient(), "https://encryption.keboola.com")

	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	encryptedMapPtr, err := encryptionapi.EncryptRequest(1234, "keboola.ex-generic-v2", mapToEncrypt).Send(ctx, c)
	assert.NoError(t, err)
	encryptedMap := *encryptedMapPtr
	assert.NotEmpty(t, encryptedMap)
	assert.True(t, encryptionapi.IsEncrypted(encryptedMap["#keyToEncrypt"]))
}

func TestError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := encryptionapi.APIClient(client.NewTestClient(), "https://encryption.keboola.com")

	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	_, err := encryptionapi.EncryptRequest(1234, "", mapToEncrypt).Send(ctx, c)
	assert.Error(t, err)
	assert.IsType(t, &encryptionapi.Error{}, err)
	assert.Contains(t, err.Error(), "The componentId parameter is required")
}
