package storageapi_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
)

func TestIsTransformationWithBlocks(t *testing.T) {
	t.Parallel()

	component := &storageapi.Component{Flags: []string{storageapi.GenericCodeBlocksUIFlag}}
	assert.True(t, component.IsTransformationWithBlocks())
}
