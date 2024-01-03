package keboola

import (
	"bytes"
	"context"
	"encoding/csv"
	jsonLib "encoding/json"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/request"
)

// CreateTableRequest creates an empty table with given columns.
func (a *AuthorizedAPI) CreateTableRequest(k TableKey, columns []string, opts ...CreateTableOption) request.APIRequest[*Table] {
	table := &Table{}
	fileName := fmt.Sprintf("create-table-%s", k.TableID.String())
	req := a.
		CreateFileResourceRequest(k.BranchID, fileName).
		WithOnSuccess(func(ctx context.Context, file *FileUploadCredentials) error {
			// Upload file with the header
			if err := writeHeaderToCSV(ctx, file, columns); err != nil {
				return fmt.Errorf("error writing header to csv: %w", err)
			}

			// Create the table from the header file
			res, err := a.CreateTableFromFileRequest(k, file.FileKey, opts...).Send(ctx)
			*table = *res
			return err
		})
	return request.NewAPIRequest(table, req)
}

// CreateTableFromFileRequest https://keboola.docs.apiary.io/#reference/tables/create-table-asynchronously/create-new-table-from-csv-file-asynchronously
func (a *AuthorizedAPI) CreateTableFromFileRequest(tableKey TableKey, fileKey FileKey, opts ...CreateTableOption) request.APIRequest[*Table] {
	// Check branch ID
	if tableKey.BranchID != fileKey.BranchID {
		return request.NewAPIRequest(&Table{}, request.NewReqDefinitionError(
			fmt.Errorf(`table (branch:%s) and file (branch:%s) must be from the same branch`, tableKey.BranchID.String(), fileKey.BranchID.String()),
		))
	}

	c := &createTableConfig{}
	for _, o := range opts {
		o.applyCreateTableOption(c)
	}

	params := request.StructToMap(c, nil)
	params["name"] = tableKey.TableID.TableName
	params["dataFileId"] = fileKey.FileID

	job := &StorageJob{}
	table := &Table{TableKey: tableKey}
	req := a.
		newRequest(StorageAPI).
		WithResult(job).
		WithPost("branch/{branchId}/buckets/{bucketId}/tables-async").
		AndPathParam("branchId", tableKey.BranchID.String()).
		AndPathParam("bucketId", tableKey.TableID.BucketID.String()).
		WithFormBody(request.ToFormBody(params)).
		WithOnSuccess(func(ctx context.Context, _ request.HTTPResponse) error {
			// Wait for storage job
			waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*5)
			defer waitCancelFn()
			return a.WaitForStorageJob(waitCtx, job)
		}).
		WithOnSuccess(func(_ context.Context, _ request.HTTPResponse) error {
			resultBytes, err := jsonLib.Marshal(job.Results)
			if err != nil {
				return fmt.Errorf(`cannot encode create table results: %w`, err)
			}
			err = jsonLib.Unmarshal(resultBytes, &table)
			if err != nil {
				return fmt.Errorf(`cannot decode create table results: %w`, err)
			}
			return nil
		})

	return request.NewAPIRequest(table, req)
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
