package storageapi

import (
	"context"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

type TableID string

func (v TableID) String() string {
	return string(v)
}

type TableKey struct {
	BranchID BranchID `json:"branchId"`
	ID       TableID  `json:"id"`
}

func (k TableKey) ObjectId() any {
	return k.ID
}

// Table https://keboola.docs.apiary.io/#reference/tables/list-tables/list-all-tables
type Table struct {
	TableKey
	Uri            string                    `json:"uri"`
	Name           string                    `json:"name"`
	DisplayName    string                    `json:"displayName"`
	PrimaryKey     []string                  `json:"primaryKey"`
	Created        Time                      `json:"created"`
	LastImportDate Time                      `json:"lastImportDate"`
	LastChangeDate Time                      `json:"lastChangeDate"`
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

func ListTablesRequest(branch BranchKey, opts ...Option) client.APIRequest[*[]*Table] {
	config := listTablesConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Table, 0)
	request := newRequest().
		WithResult(&result).
		WithGet("branch/{branchId}/tables").
		AndPathParam("branchId", branch.ID.String()).
		AndQueryParam("include", config.includeString()).
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.HTTPResponse) error {
			for _, table := range result {
				table.BranchID = branch.ID
				if table.Bucket != nil {
					table.Bucket.BranchID = branch.ID
				}
			}
			return nil
		})

	return client.NewAPIRequest(&result, request)
}