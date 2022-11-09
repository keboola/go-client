package storageapi

import (
	"bytes"
	jsonLib "encoding/json"
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
	ID             TableID          `json:"id"`
	Uri            string           `json:"uri"`
	Name           string           `json:"name"`
	DisplayName    string           `json:"displayName"`
	PrimaryKey     []string         `json:"primaryKey"`
	Created        iso8601.Time     `json:"created"`
	LastImportDate iso8601.Time     `json:"lastImportDate"`
	LastChangeDate *iso8601.Time    `json:"lastChangeDate"`
	RowsCount      uint64           `json:"rowsCount"`
	DataSizeBytes  uint64           `json:"dataSizeBytes"`
	Columns        []string         `json:"columns"`
	Metadata       []MetadataDetail `json:"metadata"`
	ColumnMetadata ColumnMetadata   `json:"columnMetadata"`
	Bucket         *Bucket          `json:"bucket"`
}

type ColumnMetadata map[string]MetadataDetail

// UnmarshalJSON implements JSON decoding.
// The API returns empty value as empty array.
func (r *ColumnMetadata) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "[]" {
		*r = ColumnMetadata{}
		return nil
	}
	// see https://stackoverflow.com/questions/43176625/call-json-unmarshal-inside-unmarshaljson-function-without-causing-stack-overflow
	type _r ColumnMetadata
	return jsonLib.Unmarshal(data, (*_r)(r))
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
		WithPost("buckets/{bucketId}/tables").
		AndPathParam("bucketId", string(table.Bucket.ID)).
		WithBody(bytes.NewReader(body.Bytes())).
		WithContentType(contentType)

	return client.NewAPIRequest(table, request)
}

// loadDataFromFileConfig contains common params to load data from file resource.
type loadDataFromFileConfig struct {
	Delimiter string `json:"delimiter,omitempty" writeoptional:"true"`
	Enclosure string `json:"enclosure,omitempty" writeoptional:"true"`
	EscapedBy string `json:"escapedBy,omitempty" writeoptional:"true"`
}

// delimiterOption specifies field delimiter used in the CSV file. Default value is ','.
type delimiterOption string

func WithDelimiter(d string) delimiterOption {
	return delimiterOption(d)
}

// enclosureOption specifies field enclosure used in the CSV file. Default value is '"'.
type enclosureOption string

func WithEnclosure(e string) enclosureOption {
	return enclosureOption(e)
}

// escapedByOption specifies escape character used in the CSV file. The default value is an empty value - no escape character is used.
// Note: you can specify either enclosure or escapedBy parameter, not both.
type escapedByOption string

func WithEscapedBy(e string) escapedByOption {
	return escapedByOption(e)
}

// CreateTableOption applies to the request for creating table from file.
type CreateTableOption interface {
	applyCreateTableOption(c *createTableConfig)
}

// createTableConfig contains params to create table from file resource.
type createTableConfig struct {
	loadDataFromFileConfig
	PrimaryKey string `json:"primaryKey,omitempty" writeoptional:"true"`
}

func (o delimiterOption) applyCreateTableOption(c *createTableConfig) {
	c.Delimiter = string(o)
}

func (o enclosureOption) applyCreateTableOption(c *createTableConfig) {
	c.Enclosure = string(o)
}

func (o escapedByOption) applyCreateTableOption(c *createTableConfig) {
	c.EscapedBy = string(o)
}

// primaryKeyOption specifies primary key of the table. Multiple columns can be separated by a comma.
type primaryKeyOption string

func (o primaryKeyOption) applyCreateTableOption(c *createTableConfig) {
	c.PrimaryKey = string(o)
}

func WithPrimaryKey(pk []string) primaryKeyOption {
	return primaryKeyOption(strings.Join(pk, ","))
}

// CreateTableFromFileRequest https://keboola.docs.apiary.io/#reference/tables/create-table-asynchronously/create-new-table-from-csv-file-asynchronously
func CreateTableFromFileRequest(bucketID string, name string, dataFileID int, opts ...CreateTableOption) client.APIRequest[*Job] {
	c := &createTableConfig{}
	for _, o := range opts {
		o.applyCreateTableOption(c)
	}

	params := client.StructToMap(c, nil)
	params["name"] = name
	params["dataFileId"] = dataFileID

	job := &Job{}
	request := newRequest().
		WithResult(job).
		WithPost("buckets/{bucketId}/tables-async").
		AndPathParam("bucketId", bucketID).
		WithFormBody(client.ToFormBody(params))

	return client.NewAPIRequest(job, request)
}

// LoadDataOption applies to the request loading data to a table.
type LoadDataOption interface {
	applyLoadDataOption(c *loadDataConfig)
}

// loadDataConfig contains params to load data to a table from file resource.
type loadDataConfig struct {
	loadDataFromFileConfig
	IncrementalLoad int      `json:"incremental,omitempty" writeoptional:"true"`
	WithoutHeaders  int      `json:"withoutHeaders,omitempty" writeoptional:"true"`
	Columns         []string `json:"columns,omitempty" writeoptional:"true"`
}

func (o delimiterOption) applyLoadDataOption(c *loadDataConfig) {
	c.Delimiter = string(o)
}

func (o enclosureOption) applyLoadDataOption(c *loadDataConfig) {
	c.Enclosure = string(o)
}

func (o escapedByOption) applyLoadDataOption(c *loadDataConfig) {
	c.EscapedBy = string(o)
}

// incrementalLoadOption decides whether the target table will be truncated before import.
type incrementalLoadOption bool

func (o incrementalLoadOption) applyLoadDataOption(c *loadDataConfig) {
	c.IncrementalLoad = 0
	if o {
		c.IncrementalLoad = 1
	}
}

func WithIncrementalLoad(i bool) incrementalLoadOption {
	return incrementalLoadOption(i)
}

// columnsHeadersOption specifies list of columns present in the CSV file.
// The first line of the file will not be treated as a header.
type columnsHeadersOption []string

func (o columnsHeadersOption) applyLoadDataOption(c *loadDataConfig) {
	c.Columns = o
}

func WithColumnsHeaders(c []string) columnsHeadersOption {
	return c
}

// withoutHeaderOption specifies if the csv file contains header. If it doesn't, columns are matched by their order.
// If this option is used, columns option is ignored.
type withoutHeaderOption bool

func (o withoutHeaderOption) applyLoadDataOption(c *loadDataConfig) {
	c.WithoutHeaders = 0
	if o {
		c.WithoutHeaders = 1
	}
}

func WithoutHeader(h bool) withoutHeaderOption {
	return withoutHeaderOption(h)
}

// LoadDataFromFileRequest https://keboola.docs.apiary.io/#reference/tables/load-data-asynchronously/import-data
func LoadDataFromFileRequest(tableID TableID, dataFileID int, opts ...LoadDataOption) client.APIRequest[*Job] {
	c := &loadDataConfig{}
	for _, o := range opts {
		o.applyLoadDataOption(c)
	}

	params := client.StructToMap(c, nil)
	params["dataFileId"] = dataFileID

	job := &Job{}
	request := newRequest().
		WithResult(job).
		WithPost("tables/{tableId}/import-async").
		AndPathParam("tableId", string(tableID)).
		WithFormBody(client.ToFormBody(params))

	return client.NewAPIRequest(job, request)
}

// GetTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/table-detail
func GetTableRequest(tableID TableID) client.APIRequest[*Table] {
	table := &Table{}
	request := newRequest().
		WithResult(table).
		WithGet("tables/{tableId}").
		AndPathParam("tableId", string(tableID))
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
