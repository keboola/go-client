package storageapi

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

type TableID string

func (v TableID) String() string {
	return string(v)
}

// Table https://keboola.docs.apiary.io/#reference/tables/list-tables/list-all-tables
type Table struct {
	ID             TableID                   `json:"id"`
	Uri            string                    `json:"uri"`
	Name           string                    `json:"name"`
	DisplayName    string                    `json:"displayName"`
	PrimaryKey     []string                  `json:"primaryKey"`
	Created        Time                      `json:"created"`
	LastImportDate Time                      `json:"lastImportDate"`
	LastChangeDate *Time                     `json:"lastChangeDate"`
	RowsCount      uint64                    `json:"rowsCount"`
	DataSizeBytes  uint64                    `json:"dataSizeBytes"`
	Columns        []string                  `json:"columns"`
	Metadata       []MetadataDetail          `json:"metadata"`
	ColumnMetadata map[string]MetadataDetail `json:"columnMetadata"`
	Bucket         *Bucket                   `json:"bucket"`
}

type listTablesConfig struct {
	include map[string]bool
}

func (v listTablesConfig) includeString() string {
	include := make([]string, 0, len(v.include))
	for k := range v.include {
		include = append(include, k)
	}
	sort.Strings(include)
	return strings.Join(include, ",")
}

type Option func(c *listTablesConfig)

func WithBuckets() Option {
	return func(c *listTablesConfig) {
		c.include["buckets"] = true
	}
}

func WithColumns() Option {
	return func(c *listTablesConfig) {
		c.include["columns"] = true
	}
}

func WithMetadata() Option {
	return func(c *listTablesConfig) {
		c.include["metadata"] = true
	}
}

func WithColumnMetadata() Option {
	return func(c *listTablesConfig) {
		c.include["columnMetadata"] = true
	}
}

// ListTablesRequest https://keboola.docs.apiary.io/#reference/tables/list-tables/list-all-tables
func ListTablesRequest(opts ...Option) client.APIRequest[*[]*Table] {
	config := listTablesConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Table, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("tables").
		AndQueryParam("include", config.includeString())

	return client.NewAPIRequest(&result, request)
}

// CreateTableRequest https://keboola.docs.apiary.io/#reference/tables/create-or-list-tables/create-new-table-from-csv-file
func CreateTableRequest(table *Table) client.APIRequest[*Table] {
	params := map[string]string{
		"name": table.Name,
	}
	if len(table.PrimaryKey) > 0 {
		params["primaryKey"] = strings.Join(table.PrimaryKey, ",")
	}

	request := newRequest().
		WithResult(table).
		WithPost(fmt.Sprintf("buckets/%s/tables", table.Bucket.ID)).
		WithMultipartBody(params, []byte(strings.Join(table.Columns, ",")))

	return client.NewAPIRequest(table, request)
}
