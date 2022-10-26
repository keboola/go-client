package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsJsonContentType(t *testing.T) {
	t.Parallel()

	assert.False(t, isJsonContentType(""))
	assert.False(t, isJsonContentType(" "))
	assert.False(t, isJsonContentType("foo"))
	assert.False(t, isJsonContentType("application/yaml"))
	assert.False(t, isJsonContentType("application/vnd.foo.api+yaml"))
	assert.False(t, isJsonContentType("application/x-resource+yaml"))
	assert.False(t, isJsonContentType("application/x-collection+yaml"))
	assert.False(t, isJsonContentType("application/json-foo"))
	assert.False(t, isJsonContentType("application/foo-json"))

	assert.True(t, isJsonContentType("application/json"))
	assert.True(t, isJsonContentType("application/vnd.foo.api+json"))
	assert.True(t, isJsonContentType("application/x-resource+json"))
	assert.True(t, isJsonContentType("application/x-collection+json"))
}
