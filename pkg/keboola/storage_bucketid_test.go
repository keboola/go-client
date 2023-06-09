package keboola

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBucketID(t *testing.T) {
	t.Parallel()

	testCases := []struct{ str, err string }{
		{str: "", err: `bucket ID cannot be empty`},
		{str: "in", err: `invalid bucket ID "in": unexpected number of fragments`},
		{str: "in.bucket.table", err: `invalid bucket ID "in.bucket.table": unexpected number of fragments`},
		{str: "in.bucket"},
		{str: "in.bucket"},
	}

	for i, tc := range testCases {
		desc := fmt.Sprintf("test case %d", i+1)
		val, err := ParseBucketID(tc.str)
		if tc.err == "" {
			assert.NoError(t, err, desc)
			assert.Equal(t, tc.str, val.String())
		} else {
			if assert.Error(t, err, desc) {
				assert.Equal(t, tc.err, err.Error(), desc)
			}
		}
	}
}
