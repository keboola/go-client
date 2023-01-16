package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsJsonContentType(t *testing.T) {
	t.Parallel()

	assert.False(t, isJSONContentType(""))
	assert.False(t, isJSONContentType(" "))
	assert.False(t, isJSONContentType("foo"))
	assert.False(t, isJSONContentType("application/yaml"))
	assert.False(t, isJSONContentType("application/vnd.foo.api+yaml"))
	assert.False(t, isJSONContentType("application/x-resource+yaml"))
	assert.False(t, isJSONContentType("application/x-collection+yaml"))
	assert.False(t, isJSONContentType("application/json-foo"))
	assert.False(t, isJSONContentType("application/foo-json"))

	assert.True(t, isJSONContentType("application/json"))
	assert.True(t, isJSONContentType("application/vnd.foo.api+json"))
	assert.True(t, isJSONContentType("application/x-resource+json"))
	assert.True(t, isJSONContentType("application/x-collection+json"))
}
