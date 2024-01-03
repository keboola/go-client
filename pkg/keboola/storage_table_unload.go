package keboola

import (
	"context"
	jsonLib "encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/request"
)

const (
	// UnloadFormatCSV generates CSV formatted according to RFC4180. This is the default format.
	UnloadFormatCSV UnloadFormat = "rfc"
	// UnloadFormatJSON is only supported in projects with the Snowflake backend.
	UnloadFormatJSON UnloadFormat = "json"
)

type UnloadFormat string

type TableUnloadRequestBuilder struct {
	tableKey TableKey
	config   unloadConfig
	api      *AuthorizedAPI
}

type unloadConfig struct {
	Limit        uint          `json:"limit,omitempty"`
	Format       UnloadFormat  `json:"format,omitempty"`
	ChangedSince string        `json:"changedSince,omitempty"`
	ChangedUntil string        `json:"changedUntil,omitempty"`
	Columns      string        `json:"columns,omitempty"`
	OrderBy      []orderBy     `json:"orderBy,omitempty"`
	WhereFilters []whereFilter `json:"whereFilters,omitempty"`
}

func (a *AuthorizedAPI) NewTableUnloadRequest(k TableKey) *TableUnloadRequestBuilder {
	return &TableUnloadRequestBuilder{
		tableKey: k,
		api:      a,
	}
}

func (b *TableUnloadRequestBuilder) Build() request.APIRequest[*StorageJob] {
	result := &StorageJob{}
	req := b.api.newRequest(StorageAPI).
		WithResult(result).
		WithMethod(http.MethodPost).
		WithURL("branch/{branchId}/tables/{tableId}/export-async").
		AndPathParam("branchId", b.tableKey.BranchID.String()).
		AndPathParam("tableId", b.tableKey.TableID.String()).
		WithJSONBody(b.config)
	return request.NewAPIRequest(result, req)
}

func (b *TableUnloadRequestBuilder) Send(ctx context.Context) (*StorageJob, error) {
	return b.Build().Send(ctx)
}

type TableUnloadJobResult struct {
	File     UnloadedFile `json:"file"`
	CacheHit bool         `json:"cacheHit"`
}

type UnloadedFile struct {
	FileKey
}

// SendAndWait the request and wait for the resulting storage job to finish.
// Once the job finishes, this returns its `results` object, which contains the created file ID.
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
	result.File.BranchID = b.tableKey.BranchID
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

// WithLimitRows the number of returned rows.
// Maximum allowed value is 1000.
// Default value is 100.
func (b *TableUnloadRequestBuilder) WithLimitRows(v uint) *TableUnloadRequestBuilder {
	b.config.Limit = v
	return b
}

// WithFormat the output file format.
// JSON format is only supported in projects with the Snowflake backend.
func (b *TableUnloadRequestBuilder) WithFormat(v UnloadFormat) *TableUnloadRequestBuilder {
	b.config.Format = v
	return b
}

// WithChangedSince sets filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func (b *TableUnloadRequestBuilder) WithChangedSince(v string) *TableUnloadRequestBuilder {
	b.config.ChangedSince = v
	return b
}

// WithChangedUntil sets filtering by import date - timestamp of import is stored within each row.
// Can be a unix timestamp or any date accepted by strtotime (https://www.php.net/manual/en/function.strtotime.php).
func (b *TableUnloadRequestBuilder) WithChangedUntil(v string) *TableUnloadRequestBuilder {
	b.config.ChangedUntil = v
	return b
}

// WithColumns sets list of columns to export. By default, all columns are exported.
func (b *TableUnloadRequestBuilder) WithColumns(v ...string) *TableUnloadRequestBuilder {
	b.config.Columns = strings.Join(v, ",")
	return b
}

func (b *TableUnloadRequestBuilder) WithOrderBy(column string, order ColumnOrder, ty ...DataType) *TableUnloadRequestBuilder {
	b.config.OrderBy = append(b.config.OrderBy, newOrderBy(column, order, ty...))
	return b
}

// WithWhere sets a where condition. If the column contains a numeric type, `ty` may be used to specify the exact type.
// `ty` should be exactly one value, or empty.
func (b *TableUnloadRequestBuilder) WithWhere(column string, op CompareOp, values []string, ty ...DataType) *TableUnloadRequestBuilder {
	b.config.WhereFilters = append(b.config.WhereFilters, newWhereFilter(column, op, values, ty...))
	return b
}
