package keboola_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/keboola/go-client/pkg/keboola"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestListTablesRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	tables, err := api.ListTablesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *tables, 0)
}

func TestTableApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketName := fmt.Sprintf("c-test_%d", rnd.Int())
	tableName := fmt.Sprintf("test_%d", rnd.Int())

	bucket := &Bucket{
		BranchID: defBranch.ID,
		BucketID: BucketID{
			Stage:      BucketStageIn,
			BucketName: bucketName,
		},
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	tableID := TableID{
		BucketID:  bucket.BucketID,
		TableName: tableName,
	}
	table := &Table{
		BranchID:   defBranch.ID,
		TableID:    tableID,
		Bucket:     bucket,
		Name:       tableName,
		Columns:    []string{"first", "second", "third", "fourth"},
		PrimaryKey: []string{"first", "fourth"},
	}

	// Create table
	res, err := api.CreateTableRequest(defBranch.ID, tableID, table.Columns).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, table.Name, res.Name)

	// List tables
	resList, err := api.ListTablesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	tableFound := false
	for _, t := range *resList {
		if t.TableID == table.TableID {
			tableFound = true
		}
	}
	assert.True(t, tableFound)

	// Get table (without table and columns metadata)
	respGet1, err := api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	respGet1.Created = iso8601.Time{}
	respGet1.LastImportDate = iso8601.Time{}
	respGet1.LastChangeDate = nil
	respGet1.Bucket.Created = iso8601.Time{}
	respGet1.Bucket.LastChangeDate = nil
	assert.Equal(t, &Table{
		BranchID:      defBranch.ID,
		TableID:       tableID,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableID.String(),
		Name:          tableName,
		DisplayName:   tableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
			BranchID:    defBranch.ID,
			BucketID:    table.Bucket.BucketID,
			DisplayName: table.Bucket.DisplayName,
			URI:         "https://" + project.StorageAPIHost() + "/v2/storage/buckets/" + tableID.BucketID.String(),
		},
		Metadata:       TableMetadata{},
		ColumnMetadata: ColumnsMetadata{},
	}, respGet1)

	// Set metadata
	resMetadata, err := api.
		CreateOrUpdateTableMetadata(
			defBranch.ID,
			tableID,
			"go-client-test",
			[]TableMetadataRequest{
				{Key: "tableMetadata1", Value: "value1"},
				{Key: "tableMetadata2", Value: "value2"},
			},
			[]ColumnMetadataRequest{
				{ColumnName: "first", Key: "columnMetadata1", Value: "value3"},
				{ColumnName: "first", Key: "columnMetadata2", Value: "value4"},
				{ColumnName: "second", Key: "columnMetadata3", Value: "value5"},
				{ColumnName: "second", Key: "columnMetadata4", Value: "value6"},
			},
		).
		Send(ctx)
	assert.NoError(t, err)

	// Check metadata response
	removeDynamicValuesFromTableMetadata(resMetadata.Metadata)
	removeDynamicValuesFromColumnsMetadata(resMetadata.ColumnMetadata)
	assert.Equal(t, &TableMetadataResponse{
		Metadata: TableMetadata{
			{Key: "tableMetadata1", Value: "value1", Provider: "go-client-test"},
			{Key: "tableMetadata2", Value: "value2", Provider: "go-client-test"},
		},
		ColumnMetadata: ColumnsMetadata{
			"first": {
				{Key: "columnMetadata1", Value: "value3", Provider: "go-client-test"},
				{Key: "columnMetadata2", Value: "value4", Provider: "go-client-test"},
			},
			"second": {
				{Key: "columnMetadata3", Value: "value5", Provider: "go-client-test"},
				{Key: "columnMetadata4", Value: "value6", Provider: "go-client-test"},
			},
		},
	}, resMetadata)

	// Get table (with table and columns metadata)
	respGet2, err := api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	removeDynamicValuesFromTableMetadata(respGet2.Metadata)
	removeDynamicValuesFromColumnsMetadata(respGet2.ColumnMetadata)
	respGet2.Created = iso8601.Time{}
	respGet2.LastImportDate = iso8601.Time{}
	respGet2.LastChangeDate = nil
	respGet2.Bucket.Created = iso8601.Time{}
	respGet2.Bucket.LastChangeDate = nil
	assert.Equal(t, &Table{
		BranchID:      defBranch.ID,
		TableID:       tableID,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableID.String(),
		Name:          tableName,
		DisplayName:   tableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
			BranchID:    defBranch.ID,
			BucketID:    table.Bucket.BucketID,
			DisplayName: table.Bucket.DisplayName,
			URI:         "https://" + project.StorageAPIHost() + "/v2/storage/buckets/" + tableID.BucketID.String(),
		},
		Metadata: TableMetadata{
			{Key: "tableMetadata1", Value: "value1", Provider: "go-client-test"},
			{Key: "tableMetadata2", Value: "value2", Provider: "go-client-test"},
		},
		ColumnMetadata: ColumnsMetadata{
			"first": {
				{Key: "columnMetadata1", Value: "value3", Provider: "go-client-test"},
				{Key: "columnMetadata2", Value: "value4", Provider: "go-client-test"},
			},
			"second": {
				{Key: "columnMetadata3", Value: "value5", Provider: "go-client-test"},
				{Key: "columnMetadata4", Value: "value6", Provider: "go-client-test"},
			},
		},
	}, respGet2)

	// Delete table
	_, err = api.DeleteTableRequest(defBranch.ID, table.TableID, WithForce()).Send(ctx)
	assert.NoError(t, err)

	// List tables again - without the deleted table
	resList, err = api.ListTablesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	tableFound = false
	for _, t := range *resList {
		if t.TableID == table.TableID {
			tableFound = true
		}
	}
	assert.False(t, tableFound)
}

func TestTableCreateLoadDataFromFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BranchID: defBranch.ID,
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(defBranch.ID, tableID, file1.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Create file
	fileName2 := fmt.Sprintf("file_%d", rnd.Int())
	file2, err := api.CreateFileResourceRequest(defBranch.ID, fileName2).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file2.ID)

	// Check rows count
	table, err := api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)

	// Upload file
	content = []byte("val2,val3\nval3,val4\nval4,val5\n")
	written, err = Upload(ctx, file2, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Load data to table - added three rows
	waitCtx2, waitCancelFn2 := context.WithTimeout(ctx, time.Minute*5)
	defer waitCancelFn2()
	job, err := api.LoadDataFromFileRequest(defBranch.ID, tableID, file2.ID, WithColumnsHeaders([]string{"col2", "col1"}), WithIncrementalLoad(true)).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx2, job))

	// Check rows count
	table, err = api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), table.RowsCount)
}

func TestTableCreateFromSlicedFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx, testproject.WithStagingStorageS3())

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketName := fmt.Sprintf("c-test_%d", rnd.Int())
	tableName := fmt.Sprintf("test_%d", rnd.Int())

	bucket := &Bucket{
		BranchID: defBranch.ID,
		BucketID: BucketID{
			Stage:      BucketStageIn,
			BucketName: bucketName,
		},
	}

	// Create bucket
	_, err = api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, bucket.BucketID)
	tableID := TableID{
		BucketID:  bucket.BucketID,
		TableName: tableName,
	}

	// Create whole file
	wholeFile, err := api.CreateFileResourceRequest(defBranch.ID, tableName).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, wholeFile.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, wholeFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create non-sliced table.
	// Table cannot be created from a sliced file (https://keboola.atlassian.net/browse/KBC-1861).
	_, err = api.CreateTableFromFileRequest(defBranch.ID, tableID, wholeFile.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)

	// Create sliced file
	slicedFile, err := api.CreateFileResourceRequest(defBranch.ID, tableName, WithIsSliced(true)).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, slicedFile.ID)

	// Upload slice 1 file
	content = []byte("val3,val4\nval5,val6\n")
	_, err = UploadSlice(ctx, slicedFile, "slice1", bytes.NewReader(content))
	assert.NoError(t, err)

	// Upload slice 2 file
	content = []byte("val7,val8\nval9,val10\n")
	_, err = UploadSlice(ctx, slicedFile, "slice2", bytes.NewReader(content))
	assert.NoError(t, err)

	// Upload manifest
	_, err = UploadSlicedFileManifest(ctx, slicedFile, []string{"slice1", "slice2"})
	assert.NoError(t, err)

	// Load data to table
	waitCtx, waitCancelFn := context.WithTimeout(ctx, time.Minute*5)
	defer waitCancelFn()
	job, err := api.LoadDataFromFileRequest(defBranch.ID, tableID, slicedFile.ID, WithIncrementalLoad(true), WithColumnsHeaders([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx, job))

	// Check rows count
	table, err = api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), table.RowsCount)
}

func TestTableCreateFromFileOtherOptions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BranchID: defBranch.ID,
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.ID)

	// Upload file
	content := []byte("'col1'&'col2'\n'val1'&'val2'\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(defBranch.ID, tableID, file1.ID, WithDelimiter("&"), WithEnclosure("'")).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(defBranch.ID, tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)
}

func TestTableUnloadRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BranchID: defBranch.ID,
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	inputFile, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, inputFile.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, inputFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(defBranch.ID, tableID, inputFile.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Unload table as CSV
	outputFileInfo, err := api.NewTableUnloadRequest(tableID).
		WithColumns("col1").
		WithChangedSince("-2 days").
		WithFormat(UnloadFormatCSV).
		WithLimitRows(100).
		WithOrderBy("col1", OrderAsc).
		WithWhere("col1", CompareEq, []string{"val1"}).
		SendAndWait(ctx, time.Minute*5)
	assert.NoError(t, err)

	// Download file
	credentials, err := api.GetFileWithCredentialsRequest(defBranch.ID, outputFileInfo.File.ID).Send(ctx)
	assert.NoError(t, err)

	data, err := downloadAllSlices(ctx, credentials)
	assert.NoError(t, err)

	row, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, [][]string{{"val1"}}, row)
}

func downloadAllSlices(ctx context.Context, file *FileDownloadCredentials) ([]byte, error) {
	if !file.IsSliced {
		return nil, fmt.Errorf("cannot download a whole file as a sliced file")
	}

	out := []byte{}

	slices, err := DownloadManifest(ctx, file)
	if err != nil {
		return nil, err
	}

	for _, slice := range slices {
		data, err := DownloadSlice(ctx, file, slice)
		if err != nil {
			return nil, err
		}
		out = append(out, data...)
	}

	return out, nil
}

func removeDynamicValuesFromTableMetadata(in TableMetadata) {
	for i := range in {
		meta := &in[i]
		meta.ID = ""
		meta.Timestamp = ""
	}
}

func removeDynamicValuesFromColumnsMetadata(in ColumnsMetadata) {
	for _, colMetadata := range in {
		for i := range colMetadata {
			item := &colMetadata[i]
			item.ID = ""
			item.Timestamp = ""
		}
	}
}
