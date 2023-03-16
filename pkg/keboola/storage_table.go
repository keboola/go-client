package keboola

import (
	"bytes"
	"context"
	"encoding/csv"
	jsonLib "encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

// Table https://keboola.docs.apiary.io/#reference/tables/list-tables/list-all-tables
type Table struct {
	ID             TableID          `json:"id"`
	URI            string           `json:"uri"`
	Name           string           `json:"name"`
	DisplayName    string           `json:"displayName"`
	SourceTable    *SourceTable     `json:"sourceTable"`
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

type SourceTable struct {
	ID      TableID       `json:"id"`
	URI     string        `json:"uri"`
	Name    string        `json:"name"`
	Project SourceProject `json:"project"`
}

type SourceProject struct {
	ID   ProjectID `json:"id"`
	Name string    `json:"name"`
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
func (a *API) ListTablesRequest(opts ...Option) client.APIRequest[*[]*Table] {
	config := listTablesConfig{include: make(map[string]bool)}
	for _, opt := range opts {
		opt(&config)
	}

	result := make([]*Table, 0)
	request := a.
		newRequest(StorageAPI).
		WithResult(&result).
		WithGet("tables").
		AndQueryParam("include", config.includeString())

	return client.NewAPIRequest(&result, request)
}

func writeHeaderToCSV(ctx context.Context, file *FileUploadCredentials, columns []string) (err error) {
	// Upload file with the header
	bw, err := NewUploadWriter(ctx, file)
	if err != nil {
		return fmt.Errorf("connecting to the bucket failed: %w", err)
	}
	defer func() {
		if closeErr := bw.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("cannot close bucket writer: %w", closeErr)
		}
	}()

	header, err := columnsToCSVHeader(columns)
	if err != nil {
		return err
	}

	_, err = bw.Write(header)
	return err
}

func columnsToCSVHeader(columns []string) ([]byte, error) {
	var str bytes.Buffer
	cw := csv.NewWriter(&str)
	if err := cw.Write(columns); err != nil {
		return nil, fmt.Errorf("error writing header to csv: %w", err)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, fmt.Errorf("error writing header to csv: %w", err)
	}
	return str.Bytes(), nil
}

// CreateTableRequest creates an empty table with given columns.
func (a *API) CreateTableRequest(tableID TableID, columns []string, opts ...CreateTableOption) client.APIRequest[*Table] {
	table := &Table{}
	request := a.
		CreateFileResourceRequest(tableID.TableName).
		WithOnSuccess(func(ctx context.Context, file *FileUploadCredentials) error {
			// Upload file with the header
			if err := writeHeaderToCSV(ctx, file, columns); err != nil {
				return fmt.Errorf("error writing header to csv: %w", err)
			}

			// Create the table from the header file
			res, err := a.CreateTableFromFileRequest(tableID, file.ID, opts...).Send(ctx)
			*table = *res
			return err
		})
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
	PrimaryKey string `json:"primaryKey,omitempty" writeoptional:"true"`
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
func (a *API) CreateTableFromFileRequest(tableID TableID, dataFileID int, opts ...CreateTableOption) client.APIRequest[*Table] {
	c := &createTableConfig{}
	for _, o := range opts {
		o.applyCreateTableOption(c)
	}

	params := client.StructToMap(c, nil)
	params["name"] = tableID.TableName
	params["dataFileId"] = dataFileID

	job := &StorageJob{}
	table := &Table{}
	request := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithPost("buckets/{bucketId}/tables-async").
		AndPathParam("bucketId", tableID.BucketID.String()).
		WithFormBody(client.ToFormBody(params)).
		WithOnSuccess(func(ctx context.Context, _ client.HTTPResponse) error {
			// Wait for storage job
			waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*1)
			defer waitCancelFn()
			return a.WaitForStorageJob(waitCtx, job)
		}).
		WithOnSuccess(func(_ context.Context, _ client.HTTPResponse) error {
			bytes, err := jsonLib.Marshal(job.Results)
			if err != nil {
				return fmt.Errorf(`cannot encode create table results: %w`, err)
			}
			err = jsonLib.Unmarshal(bytes, &table)
			if err != nil {
				return fmt.Errorf(`cannot decode create table results: %w`, err)
			}
			return nil
		})

	return client.NewAPIRequest(table, request)
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
func (a *API) LoadDataFromFileRequest(tableID TableID, dataFileID int, opts ...LoadDataOption) client.APIRequest[*StorageJob] {
	c := &loadDataConfig{}
	for _, o := range opts {
		o.applyLoadDataOption(c)
	}

	params := client.StructToMap(c, nil)
	params["dataFileId"] = dataFileID

	job := &StorageJob{}
	request := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithPost("tables/{tableId}/import-async").
		AndPathParam("tableId", tableID.String()).
		WithFormBody(client.ToFormBody(params))

	return client.NewAPIRequest(job, request)
}

// GetTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/table-detail
func (a *API) GetTableRequest(tableID TableID) client.APIRequest[*Table] {
	table := &Table{}
	request := a.
		newRequest(StorageAPI).
		WithResult(table).
		WithGet("tables/{tableId}").
		AndPathParam("tableId", tableID.String())
	return client.NewAPIRequest(table, request)
}

// DeleteTableRequest https://keboola.docs.apiary.io/#reference/tables/manage-tables/drop-table
func (a *API) DeleteTableRequest(tableID TableID, opts ...DeleteOption) client.APIRequest[client.NoResult] {
	c := &deleteConfig{
		force: false,
	}
	for _, opt := range opts {
		opt(c)
	}

	request := a.
		newRequest(StorageAPI).
		WithDelete("tables/{tableId}").
		WithOnError(ignoreResourceNotFoundError()).
		AndPathParam("tableId", tableID.String())

	if c.force {
		request = request.AndQueryParam("force", "true")
	}

	return client.NewAPIRequest(client.NoResult{}, request)
}

type TableUnloadRequestBuilder struct {
	tableID TableID
	config  unloadConfig
	api     *API
}

type UnloadFormat string

const (
	// CSV formatted according to RFC4180. This is the default format.
	UnloadFormatCSV UnloadFormat = "rfc"
	// JSON format is only supported in projects with the Snowflake backend.
	UnloadFormatJSON UnloadFormat = "json"
)

type unloadConfig struct {
	Limit        uint          `json:"limit,omitempty"`
	Format       UnloadFormat  `json:"format,omitempty"`
	ChangedSince string        `json:"changedSince,omitempty"`
	ChangedUntil string        `json:"changedUntil,omitempty"`
	Columns      string        `json:"columns,omitempty"`
	OrderBy      []orderBy     `json:"orderBy,omitempty"`
	WhereFilters []whereFilter `json:"whereFilters,omitempty"`
}

func (a *API) NewTableUnloadRequest(tableID TableID) *TableUnloadRequestBuilder {
	return &TableUnloadRequestBuilder{
		tableID: tableID,
		api:     a,
	}
}

func (b *TableUnloadRequestBuilder) Build() client.APIRequest[*StorageJob] {
	result := &StorageJob{}
	request := b.api.newRequest(StorageAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("tables/{tableId}/export-async").
		AndPathParam("tableId", b.tableID.String()).
		WithJSONBody(b.config)
	return client.NewAPIRequest(result, request)
}

func (b *TableUnloadRequestBuilder) Send(ctx context.Context) (*StorageJob, error) {
	return b.Build().Send(ctx)
}

type TableUnloadJobResult struct {
	File     UnloadedFile `json:"file"`
	CacheHit bool         `json:"cacheHit"`
}

type UnloadedFile struct {
	Id int `json:"id"`
}

func (b *TableUnloadRequestBuilder) SendAndWait(ctx context.Context, timeout time.Duration) (*TableUnloadJobResult, error) {
	// send request
	job, err := b.Send(ctx)
	if err != nil {
		return nil, err
	}

	// wait for job
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = b.api.WaitForStorageJob(timeoutCtx, job)
	if err != nil {
		return nil, err
	}

	// parse result
	result := &TableUnloadJobResult{}
	data, err := jsonLib.Marshal(job.Results)
	if err != nil {
		return nil, err
	}
	err = jsonLib.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Limit the number of returned rows.
//
// Maximum allowed value is 1000.
//
// Default value is 100.
func (b *TableUnloadRequestBuilder) WithLimitRows(v uint) *TableUnloadRequestBuilder {
	b.config.Limit = v
	return b
}

// Set the output file format.
//
// JSON format is only supported in projects with the Snowflake backend.
func (b *TableUnloadRequestBuilder) WithFormat(v UnloadFormat) *TableUnloadRequestBuilder {
	b.config.Format = v
	return b
}

// Filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func (b *TableUnloadRequestBuilder) WithChangedSince(v string) *TableUnloadRequestBuilder {
	b.config.ChangedSince = v
	return b
}

// Filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func (b *TableUnloadRequestBuilder) WithChangedUntil(v string) *TableUnloadRequestBuilder {
	b.config.ChangedUntil = v
	return b
}

// List of columns to export. By default all columns are exported.
func (b *TableUnloadRequestBuilder) WithColumns(v ...string) *TableUnloadRequestBuilder {
	b.config.Columns = strings.Join(v, ",")
	return b
}

func (b *TableUnloadRequestBuilder) WithOrderBy(column string, order ColumnOrder, ty ...DataType) *TableUnloadRequestBuilder {
	b.config.OrderBy = append(b.config.OrderBy, newOrderBy(column, order, ty...))
	return b
}

// If the column contains a numeric type, `ty` may be used to specify the exact type.
//
// `ty` should be exactly one value, or empty.
func (b *TableUnloadRequestBuilder) WithWhere(column string, op CompareOp, values []string, ty ...DataType) *TableUnloadRequestBuilder {
	b.config.WhereFilters = append(b.config.WhereFilters, newWhereFilter(column, op, values, ty...))
	return b
}
