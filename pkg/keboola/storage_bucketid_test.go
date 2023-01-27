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
		{str: "in.c-bucket.table", err: `invalid bucket ID "in.c-bucket.table": unexpected number of fragments`},
		{str: "in.bucket"}, // the external bucket does not have a "c-" prefix
		{str: "in.c-", err: `invalid bucket ID "in.c-": bucket ID cannot be empty`},
		{str: "in.c-bucket"},
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

func TestParseBucketIDExpectMagicPrefix(t *testing.T) {
	t.Parallel()

	testCases := []struct{ str, err string }{
		{str: "", err: `bucket ID cannot be empty`},
		{str: "in", err: `invalid bucket ID "in": unexpected number of fragments`},
		{str: "in.c-bucket.table", err: `invalid bucket ID "in.c-bucket.table": unexpected number of fragments`},
		{str: "in.bucket", err: `invalid bucket ID "in.bucket": missing expected prefix "c-"`},
		{str: "in.c-", err: `invalid bucket ID "in.c-": bucket ID cannot be empty`},
		{str: "in.c-bucket"},
	}

	for i, tc := range testCases {
		desc := fmt.Sprintf("test case %d", i+1)
		val, err := ParseBucketIDExpectMagicPrefix(tc.str)
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
