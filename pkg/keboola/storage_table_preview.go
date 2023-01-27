package keboola

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

type TablePreview struct {
	Columns []string
	Rows    [][]string
}

type previewDataConfig struct {
	limit        uint
	changedSince *string
	changedUntil *string
	columns      []string
	whereFilters []whereFilter
	orderBy      []orderBy
}

type PreviewOption interface {
	applyPreviewOption(c *previewDataConfig)
}

type whereFilter struct {
	column   string
	operator compareOp
	values   []string
	dataType *dataType
}

type orderBy struct {
	column   string
	order    order
	dataType *dataType
}

type order string

const (
	OrderAsc  order = "ASC"
	OrderDesc order = "DESC"
)

type compareOp string

const (
	CompareEq compareOp = "eq"
	CompareNe compareOp = "ne"
	CompareGt compareOp = "gt"
	CompareGe compareOp = "ge"
	CompareLt compareOp = "lt"
	CompareLe compareOp = "le"
)

type dataType string

const (
	// For numbers without a decimal point (Snowflake, Teradata, Bigquery).
	TypeInteger dataType = "INTEGER"
	// For number with a decimal point (Snowflake, Bigquery).
	TypeDouble dataType = "DOUBLE"
	// For number without a decimal point (Synapse, Bigquery).
	TypeBigInt dataType = "BIGINT"
	// For number with a decimal point (Synapse, Teradata, Bigquery).
	TypeReal dataType = "REAL"
	// For numbers (Exasol, Bigquery).
	TypeDecimal dataType = "DECIMAL"
)

type whereFilterBuilder struct {
	whereFilters []whereFilter
}

func newWhereFilter(column string, op compareOp, values []string, ty ...dataType) whereFilter {
	var typeName *dataType
	if len(ty) > 1 {
		panic("where filter `ty` parameter only accepts a single value")
	}
	if len(ty) > 0 {
		typeName = &ty[0]
	}

	return whereFilter{
		column:   column,
		operator: op,
		values:   values,
		dataType: typeName,
	}
}

func valuesToString(values ...any) []string {
	out := []string{}
	for _, v := range values {
		out = append(out, fmt.Sprintf("%v", v))
	}
	return out
}

// If the column contains a numeric type, `ty` may be used to specify the exact type.
//
// `ty` should be exactly one value, or empty.
func WithWhere(column string, op compareOp, values []any, ty ...dataType) *whereFilterBuilder {
	return &whereFilterBuilder{
		whereFilters: []whereFilter{
			newWhereFilter(column, op, valuesToString(values...), ty...),
		},
	}
}

func (b *whereFilterBuilder) And(column string, op compareOp, values []any, ty ...dataType) *whereFilterBuilder {
	b.whereFilters = append(b.whereFilters, newWhereFilter(column, op, valuesToString(values...), ty...))
	return b
}

func (b *whereFilterBuilder) applyPreviewOption(c *previewDataConfig) {
	c.whereFilters = append(c.whereFilters, b.whereFilters...)
}

type orderByBuilder struct {
	orderBy []orderBy
}

func newOrderBy(column string, order order, ty ...dataType) orderBy {
	var typeName *dataType
	if len(ty) > 1 {
		panic("order by filter `ty` parameter only accepts a single value")
	}
	if len(ty) > 0 {
		typeName = &ty[0]
	}

	return orderBy{
		column:   column,
		order:    order,
		dataType: typeName,
	}
}

func WithOrderBy(column string, order order, ty ...dataType) *orderByBuilder {
	return &orderByBuilder{
		orderBy: []orderBy{
			newOrderBy(column, order, ty...),
		},
	}
}

func (b *orderByBuilder) And(column string, order order, ty ...dataType) *orderByBuilder {
	b.orderBy = append(b.orderBy, newOrderBy(column, order, ty...))
	return b
}

func (b *orderByBuilder) applyPreviewOption(c *previewDataConfig) {
	c.orderBy = append(c.orderBy, b.orderBy...)
}

type withLimitRows uint

// Limit the number of returned rows.
//
// Maximum allowed value is 1000.
//
// Default value is 100.
func WithLimitRows(value uint) withLimitRows {
	return withLimitRows(value)
}

func (v withLimitRows) applyPreviewOption(c *previewDataConfig) {
	c.limit = uint(v)
}

type withChangedSince string

// Filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func WithChangedSince(value string) withChangedSince {
	return withChangedSince(value)
}

func (v withChangedSince) applyPreviewOption(c *previewDataConfig) {
	str := string(v)
	c.changedSince = &str
}

type withChangedUntil string

// Filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func WithChangedUntil(value string) withChangedUntil {
	return withChangedUntil(value)
}

func (v withChangedUntil) applyPreviewOption(c *previewDataConfig) {
	str := string(v)
	c.changedUntil = &str
}

type withExportColumns []string

// List of columns to export. By default all columns are exported.
func WithExportColumns(columns ...string) withExportColumns {
	return withExportColumns(columns)
}

func (v withExportColumns) applyPreviewOption(c *previewDataConfig) {
	c.columns = ([]string)(v)
}

func (c *previewDataConfig) toQueryParams() map[string]string {
	out := make(map[string]string)
	for i, filter := range c.whereFilters {
		out[fmt.Sprintf("whereFilters[%d][column]", i)] = filter.column
		out[fmt.Sprintf("whereFilters[%d][operator]", i)] = string(filter.operator)
		for j, value := range filter.values {
			out[fmt.Sprintf("whereFilters[%d][values][%d]", i, j)] = value
		}
		if filter.dataType != nil {
			out[fmt.Sprintf("whereFilters[%d][dataType]", i)] = string(*filter.dataType)
		}
	}
	for i, orderBy := range c.orderBy {
		out[fmt.Sprintf("orderBy[%d][column]", i)] = orderBy.column
		out[fmt.Sprintf("orderBy[%d][order]", i)] = string(orderBy.order)
		if orderBy.dataType != nil {
			out[fmt.Sprintf("orderBy[%d][dataType]", i)] = string(*orderBy.dataType)
		}
	}
	out["limit"] = fmt.Sprintf("%d", c.limit)
	if c.changedSince != nil {
		out["changedSince"] = *c.changedSince
	}
	if c.changedUntil != nil {
		out["changedUntil"] = *c.changedUntil
	}
	if len(c.columns) > 0 {
		out["columns"] = strings.Join(c.columns, ",")
	}
	return out
}

func (a *API) PreviewTableRequest(tableID TableID, opts ...PreviewOption) client.APIRequest[*TablePreview] {
	config := previewDataConfig{}
	for _, opt := range opts {
		opt.applyPreviewOption(&config)
	}

	data := &TablePreview{}
	responseBytes := []byte{}
	request := a.
		newRequest(StorageAPI).
		WithResult(&responseBytes).
		WithGet("tables/{tableId}/data-preview").
		AndPathParam("tableId", tableID.String()).
		WithQueryParams(config.toQueryParams()).
		WithOnSuccess(func(ctx context.Context, response client.HTTPResponse) error {
			records, err := csv.NewReader(bytes.NewReader(responseBytes)).ReadAll()
			if err != nil {
				return fmt.Errorf("failed to read body csv: %w", err)
			}

			data.Columns = records[0]
			data.Rows = records[1:]

			return nil
		})

	return client.NewAPIRequest(data, request)
}
