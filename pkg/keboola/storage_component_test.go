package keboola_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/keboola"
)

func TestIsTransformationWithBlocks(t *testing.T) {
	t.Parallel()

	component := &keboola.Component{Flags: []string{keboola.GenericCodeBlocksUIFlag}}
	assert.True(t, slices.Contains(component.Flags, keboola.GenericCodeBlocksUIFlag))
}
