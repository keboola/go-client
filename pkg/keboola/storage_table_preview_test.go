package keboola

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreviewTableRequestOptions(t *testing.T) {
	t.Parallel()

	var opts []PreviewOption
	opts = append(opts,
		WithLimitRows(200),
		WithChangedSince("-4 days"),
		WithChangedUntil("-2 days"),
		WithExportColumns("a", "b"),
		WithWhere("a", CompareEq, []string{"value"}),
		WithWhere("b", CompareGt, []int{100}, TypeInteger),
		WithOrderBy("a", OrderAsc),
		WithOrderBy("b", OrderDesc, TypeInteger),
	)

	config := previewDataConfig{}
	for _, opt := range opts {
		opt.applyPreviewOption(&config)
	}

	changedSince := "-4 days"
	changedUntil := "-2 days"
	dataType := TypeInteger
	assert.Equal(t,
		previewDataConfig{
			limit:        200,
			changedSince: &changedSince,
			changedUntil: &changedUntil,
			columns:      []string{"a", "b"},
			whereFilters: []whereFilter{{
				Column:   "a",
				Operator: "eq",
				Values:   []string{"value"},
			}, {
				Column:   "b",
				Operator: "gt",
				Values:   []string{"100"},
				DataType: &dataType,
			}},
			orderBy: []orderBy{{
				Column: "a",
				Order:  "ASC",
			}, {
				Column:   "b",
				Order:    "DESC",
				DataType: &dataType,
			}},
		},
		config,
	)

	assert.Equal(t,
		config.toQueryParams(),
		map[string]string{
			"changedSince": "-4 days",
			"changedUntil": "-2 days",
			"columns":      "a,b",
			"limit":        "200",

			"orderBy[0][column]":   "a",
			"orderBy[0][order]":    "ASC",
			"orderBy[1][column]":   "b",
			"orderBy[1][dataType]": "INTEGER",
			"orderBy[1][order]":    "DESC",

			"whereFilters[0][column]":    "a",
			"whereFilters[0][operator]":  "eq",
			"whereFilters[0][values][0]": "value",

			"whereFilters[1][column]":    "b",
			"whereFilters[1][dataType]":  "INTEGER",
			"whereFilters[1][operator]":  "gt",
			"whereFilters[1][values][0]": "100",
		},
	)

	query := make(url.Values)
	for k, v := range config.toQueryParams() {
		query.Set(k, v)
	}
	queryString, err := url.QueryUnescape(query.Encode())
	assert.NoError(t, err)
	assert.Equal(t,
		"changedSince=-4 days"+
			"&changedUntil=-2 days"+
			"&columns=a,b"+
			"&limit=200"+
			"&orderBy[0][column]=a"+
			"&orderBy[0][order]=ASC"+
			"&orderBy[1][column]=b"+
			"&orderBy[1][dataType]=INTEGER"+
			"&orderBy[1][order]=DESC"+
			"&whereFilters[0][column]=a"+
			"&whereFilters[0][operator]=eq"+
			"&whereFilters[0][values][0]=value"+
			"&whereFilters[1][column]=b"+
			"&whereFilters[1][dataType]=INTEGER"+
			"&whereFilters[1][operator]=gt"+
			"&whereFilters[1][values][0]=100",
		queryString,
	)
}

func TestPreviewTable_ParseColumnOrder(t *testing.T) {
	t.Parallel()

	_, err := ParseColumnOrder("unknown")
	assert.Error(t, err)

	type testCase struct {
		input    string
		expected ColumnOrder
	}

	cases := []testCase{
		{input: "ASC", expected: OrderAsc},
		{input: "DESC", expected: OrderDesc},
	}

	for _, c := range cases {
		actual, err := ParseColumnOrder(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}

func TestPreviewTable_ParseDataType(t *testing.T) {
	t.Parallel()

	_, err := ParseDataType("unknown")
	assert.Error(t, err)

	type testCase struct {
		input    string
		expected DataType
	}

	cases := []testCase{
		{input: "INTEGER", expected: TypeInteger},
		{input: "DOUBLE", expected: TypeDouble},
		{input: "BIGINT", expected: TypeBigInt},
		{input: "REAL", expected: TypeReal},
		{input: "DECIMAL", expected: TypeDecimal},
	}

	for _, c := range cases {
		actual, err := ParseDataType(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}

func TestPreviewTable_ParseCompareOp(t *testing.T) {
	t.Parallel()

	_, err := ParseCompareOp("unknown")
	assert.Error(t, err)

	type testCase struct {
		input    string
		expected CompareOp
	}

	cases := []testCase{
		{input: "eq", expected: CompareEq},
		{input: "ne", expected: CompareNe},
		{input: "lt", expected: CompareLt},
		{input: "le", expected: CompareLe},
		{input: "gt", expected: CompareGt},
		{input: "ge", expected: CompareGe},
		{input: "=", expected: CompareEq},
		{input: "!=", expected: CompareNe},
		{input: "<", expected: CompareLt},
		{input: "<=", expected: CompareLe},
		{input: ">", expected: CompareGt},
		{input: ">=", expected: CompareGe},
	}

	for _, c := range cases {
		actual, err := ParseCompareOp(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}

func TestPreviewTableRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rand.Int()),
	}
	bucket := &Bucket{
		BucketKey: BucketKey{
			BranchID: defBranch.ID,
			BucketID: bucketID,
		},
	}

	tableKey := TableKey{
		BranchID: defBranch.ID,
		TableID: TableID{
			BucketID:  bucketID,
			TableName: fmt.Sprintf("table_%d", rand.Int()),
		},
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rand.Int())
	file1, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.FileID)

	// Upload file
	content := []byte("id,value\n")
	for i := range 100 {
		content = append(content, fmt.Sprintf("%d,%d\n", i, i)...)
	}
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableKey, file1.FileKey, WithPrimaryKey([]string{"id"})).Send(ctx)
	assert.NoError(t, err)

	// Preview table
	preview, err := api.PreviewTableRequest(tableKey,
		WithWhere("value", "ge", []int{10}, TypeInteger),
		WithWhere("value", CompareLe, []int{15}, TypeInteger),
		WithOrderBy("value", OrderDesc, TypeInteger),
	).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t,
		&TablePreview{
			Columns: []string{"id", "value"},
			Rows: [][]string{
				{"15", "15"},
				{"14", "14"},
				{"13", "13"},
				{"12", "12"},
				{"11", "11"},
				{"10", "10"},
			},
		},
		preview,
	)
}
