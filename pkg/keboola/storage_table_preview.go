// nolint: structtag
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
	Column   string    `json:"column,omitempty"`
	Operator CompareOp `json:"operator,omitempty"`
	Values   []string  `json:"values,omitempty"`
	DataType *DataType `json:"dataType,omitempty"`
}

type orderBy struct {
	Column   string      `json:"column,omitempty"`
	Order    ColumnOrder `json:"order,omitempty"`
	DataType *DataType   `json:"dataType,omitempty"`
}

type ColumnOrder string

const (
	OrderAsc  ColumnOrder = "ASC"
	OrderDesc ColumnOrder = "DESC"
)

// ParseColumnOrder parses a column order from a string.
//
// Available column order types:
//
//	 name | constant name
//	------|---------------
//	 ASC  | OrderAsc
//	 DESC | OrderDesc
//
// If you know the column order ahead of time, you can use the associated
// constant instead of parsing it from a string.
func ParseColumnOrder(s string) (ColumnOrder, error) {
	v := ColumnOrder(strings.ToUpper(s))
	switch v {
	case OrderAsc, OrderDesc:
		return v, nil
	default:
		return "", fmt.Errorf(`invalid column order "%s"`, s)
	}
}

type CompareOp string

const (
	CompareEq CompareOp = "eq"
	CompareNe CompareOp = "ne"
	CompareGt CompareOp = "gt"
	CompareGe CompareOp = "ge"
	CompareLt CompareOp = "lt"
	CompareLe CompareOp = "le"
)

// ParseCompareOp parses a comparison operator from a string.
//
// Available comparison operators:
//
//	 identifier | symbol | constant name
//	------------|--------|---------------
//	 eq         | =      | CompareEq
//	 ne         | !=     | CompareNe
//	 lt         | <      | CompareLt
//	 le         | <=     | CompareLe
//	 gt         | >      | CompareGt
//	 ge         | >=     | CompareGe
//
// This function accepts either the identifier or the symbol as valid input.
// If you know the comparison operator ahead of time, you can use the associated
// constant instead of parsing it from a string.
func ParseCompareOp(s string) (CompareOp, error) {
	s = strings.ToLower(s)
	switch s {
	case "=", string(CompareEq):
		return CompareEq, nil
	case "!=", string(CompareNe):
		return CompareNe, nil
	case "<", string(CompareLt):
		return CompareLt, nil
	case "<=", string(CompareLe):
		return CompareLe, nil
	case ">", string(CompareGt):
		return CompareGt, nil
	case ">=", string(CompareGe):
		return CompareGe, nil
	default:
		return "", fmt.Errorf(`invalid comparison operator "%s"`, s)
	}
}

type DataType string

const (
	// For numbers without a decimal point (Snowflake, Teradata, Bigquery).
	TypeInteger DataType = "INTEGER"
	// For number with a decimal point (Snowflake, Bigquery).
	TypeDouble DataType = "DOUBLE"
	// For number without a decimal point (Synapse, Bigquery).
	TypeBigInt DataType = "BIGINT"
	// For number with a decimal point (Synapse, Teradata, Bigquery).
	TypeReal DataType = "REAL"
	// For numbers (Exasol, Bigquery).
	TypeDecimal DataType = "DECIMAL"
)

// ParseDataType parses a numeric data type from a string.
//
// Available data types:
//
//	 type    | constant name
//	---------|---------------
//	 INTEGER | TypeInteger
//	 DOUBLE  | TypeDouble
//	 BIGINT  | TypeBigInt
//	 REAL    | TypeReal
//	 DECIMAL | TypeDecimal
//
// If you know the data type ahead of time, you can use the associated
// constant instead of parsing it from a string.
func ParseDataType(s string) (DataType, error) {
	v := DataType(strings.ToUpper(s))
	switch v {
	case TypeInteger, TypeDouble, TypeBigInt, TypeReal, TypeDecimal:
		return v, nil
	default:
		return "", fmt.Errorf(`invalid data type "%s"`, s)
	}
}

func newWhereFilter(column string, op CompareOp, values []string, ty ...DataType) whereFilter {
	var typeName *DataType
	if len(ty) > 1 {
		panic("where filter `ty` parameter only accepts a single value")
	}
	if len(ty) > 0 {
		typeName = &ty[0]
	}

	return whereFilter{
		Column:   column,
		Operator: op,
		Values:   values,
		DataType: typeName,
	}
}

func valuesToString[T any](values ...T) []string {
	out := []string{}
	for _, v := range values {
		out = append(out, fmt.Sprintf("%v", v))
	}
	return out
}

// If the column contains a numeric type, `ty` may be used to specify the exact type.
//
// `ty` should be exactly one value, or empty.
func WithWhere[T any](column string, op CompareOp, values []T, ty ...DataType) whereFilter {
	return newWhereFilter(column, op, valuesToString(values...), ty...)
}

func (v whereFilter) applyPreviewOption(c *previewDataConfig) {
	c.whereFilters = append(c.whereFilters, v)
}

func newOrderBy(column string, order ColumnOrder, ty ...DataType) orderBy {
	var typeName *DataType
	if len(ty) > 1 {
		panic("order by filter `ty` parameter only accepts a single value")
	}
	if len(ty) > 0 {
		typeName = &ty[0]
	}

	return orderBy{
		Column:   column,
		Order:    order,
		DataType: typeName,
	}
}

func WithOrderBy(column string, order ColumnOrder, ty ...DataType) orderBy {
	return newOrderBy(column, order, ty...)
}

func (v orderBy) applyPreviewOption(c *previewDataConfig) {
	c.orderBy = append(c.orderBy, v)
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
		out[fmt.Sprintf("whereFilters[%d][column]", i)] = filter.Column
		out[fmt.Sprintf("whereFilters[%d][operator]", i)] = string(filter.Operator)
		for j, value := range filter.Values {
			out[fmt.Sprintf("whereFilters[%d][values][%d]", i, j)] = value
		}
		if filter.DataType != nil {
			out[fmt.Sprintf("whereFilters[%d][dataType]", i)] = string(*filter.DataType)
		}
	}
	for i, orderBy := range c.orderBy {
		out[fmt.Sprintf("orderBy[%d][column]", i)] = orderBy.Column
		out[fmt.Sprintf("orderBy[%d][order]", i)] = string(orderBy.Order)
		if orderBy.DataType != nil {
			out[fmt.Sprintf("orderBy[%d][dataType]", i)] = string(*orderBy.DataType)
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
