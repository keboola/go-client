package keboola

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableID(t *testing.T) {
	t.Parallel()

	testCases := []struct{ str, err string }{
		{str: "", err: `table ID cannot be empty`},
		{str: "in", err: `invalid table ID "in": unexpected number of fragments`},
		{str: "in.bucket", err: `invalid table ID "in.bucket": unexpected number of fragments`},
		{str: "in.c-", err: `invalid table ID "in.c-": unexpected number of fragments`},
		{str: "in.c-bucket", err: `invalid table ID "in.c-bucket": unexpected number of fragments`},
		{str: "in.c-bucket.", err: `invalid table ID "in.c-bucket.": table ID cannot be empty`},
		{str: "in.c-bucket.table"},
	}

	for i, tc := range testCases {
		desc := fmt.Sprintf("test case %d", i+1)
		val, err := ParseTableID(tc.str)
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

func TestTableKey_String(t *testing.T) {
	t.Parallel()
	tableKey := TableKey{BranchID: 123, TableID: MustParseTableID("in.bucket.table")}
	assert.Equal(t, "123/in.bucket.table", tableKey.String())
}
