package keboola

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPreviewTableRequestOptions(t *testing.T) {
	t.Parallel()

	var opts []PreviewOption
	opts = append(opts,
		WithLimitRows(200),
		WithChangedSince("-4 days"),
		WithChangedUntil("-2 days"),
		WithExportColumns("a", "b"),
		Where("a", CompareEq, []any{"value"}).
			And("b", CompareGt, []any{100}, TypeInteger),
		OrderBy("a", OrderAsc).
			And("b", OrderDesc, TypeInteger),
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
				column:   "a",
				operator: "eq",
				values:   []string{"value"},
			}, {
				column:   "b",
				operator: "gt",
				values:   []string{"100"},
				dataType: &dataType,
			}},
			orderBy: []orderBy{{
				column: "a",
				order:  "ASC",
			}, {
				column:   "b",
				order:    "DESC",
				dataType: &dataType,
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

// TODO: actual request test according to `./storage_table_test.go:348`

func TestPreviewTableRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	api := APIClientForAnEmptyProject(t, ctx)

	rand.Seed(time.Now().Unix())

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("bucket_%d", rand.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rand.Int()),
	}
	bucket := &Bucket{
		ID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rand.Int())
	file1 := &File{
		IsPermanent: false,
		IsSliced:    false,
		IsEncrypted: false,
		Name:        fileName1,
	}
	_, err = api.CreateFileResourceRequest(file1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.ID)

	// Upload file
	content := []byte("id,value\n")
	for i := 0; i < 100; i++ {
		content = append(content, fmt.Sprintf("%d,%d\n", i, i)...)
	}
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableID, file1.ID, WithPrimaryKey([]string{"id"})).Send(ctx)
	assert.NoError(t, err)

	// Preview table
	preview, err := api.PreviewTableRequest(tableID,
		Where("value", CompareGe, []any{10}, TypeInteger).
			And("value", CompareLe, []any{15}, TypeInteger),
		OrderBy("value", OrderDesc, TypeInteger),
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
