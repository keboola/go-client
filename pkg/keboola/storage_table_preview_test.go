package keboola

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTablePreviewOptions(t *testing.T) {
	t.Parallel()

	var opts []PreviewOption
	opts = append(opts,
		WithLimitRows(200),
		WithChangedSince("-4 days"),
		WithChangedUntil("-2 days"),
		WithExportColumns("a", "b"),
		Where("a", CompareEq, []string{"value"}).
			And("b", CompareGt, Values(100), TypeInteger),
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
