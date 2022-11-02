package storageapi

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"sort"
	"strings"

	"github.com/relvacode/iso8601"

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
	Created        iso8601.Time              `json:"created"`
	LastImportDate iso8601.Time              `json:"lastImportDate"`
	LastChangeDate *iso8601.Time             `json:"lastChangeDate"`
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
	body := bytes.NewBufferString("")
	mp := multipart.NewWriter(body)

	err := mp.WriteField("name", table.Name)
	if err != nil {
		panic(fmt.Errorf(`could not add param "name" with value "%s" to multipart: %w`, table.Name, err))
	}
	if len(table.PrimaryKey) > 0 {
		primaryKeyColumns := strings.Join(table.PrimaryKey, ",")
		err := mp.WriteField("primaryKey", primaryKeyColumns)
		if err != nil {
			panic(fmt.Errorf(`could not add param "primaryKey" with value "%s" to multipart: %w`, primaryKeyColumns, err))
		}
	}

	// Add csv file with columns definition
	csvData := []byte(strings.Join(table.Columns, ","))
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="data"; filename="data.csv"`)
	h.Set("Content-Type", "text/csv")
	wr, err := mp.CreatePart(h)
	if err != nil {
		panic(fmt.Errorf(`could not add binary to multipart: %w`, err))
	}
	_, err = io.Copy(wr, bytes.NewBuffer(csvData))
	if err != nil {
		panic(fmt.Errorf(`could not write binary to multipart: %w`, err))
	}

	contentType := fmt.Sprintf("multipart/form-data;boundary=%v", mp.Boundary())
	err = mp.Close()
	if err != nil {
		panic(fmt.Errorf(`could not close multipart: %w`, err))
	}

	request := newRequest().
		WithResult(table).
		WithPost(fmt.Sprintf("buckets/%s/tables", table.Bucket.ID)).
		WithBody(bytes.NewReader(body.Bytes())).
		WithContentType(contentType)

	return client.NewAPIRequest(table, request)
}

// DeleteTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/drop-table
func DeleteTableRequest(tableID TableID, opts ...DeleteOption) client.APIRequest[client.NoResult] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	request := newRequest().
		WithDelete("tables/{tableId}").
		AndPathParam("tableId", string(tableID))
	if c.force {
		request = request.AndQueryParam("force", "true")
	}
	return client.NewAPIRequest(client.NoResult{}, request)
}
