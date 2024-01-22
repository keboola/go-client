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

const (
	DefaultNumber = "38,0"
	DefaultString = "16777216"
)

func TestTableKey_BucketKey(t *testing.T) {
	t.Parallel()

	tableKey := TableKey{
		BranchID: 123,
		TableID: TableID{
			BucketID: BucketID{
				Stage:      BucketStageIn,
				BucketName: "my-bucket",
			},
			TableName: fmt.Sprintf("test_%d", rnd.Int()),
		},
	}

	assert.Equal(t, BucketKey{
		BranchID: 123,
		BucketID: BucketID{
			Stage:      BucketStageIn,
			BucketName: "my-bucket",
		},
	}, tableKey.BucketKey())
}

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

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	table := &Table{
		TableKey:   tableKey,
		Bucket:     bucket,
		Name:       tableKey.TableID.TableName,
		Columns:    []string{"first", "second", "third", "fourth"},
		PrimaryKey: []string{"first", "fourth"},
	}

	// Create table
	res, err := api.CreateTableRequest(tableKey, table.Columns).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, table.Name, res.Name)

	// List tables
	resList, err := api.ListTablesRequest(defBranch.ID).Send(ctx)
	assert.NoError(t, err)
	tableFound := false
	for _, resTable := range *resList {
		assert.Equal(t, resTable.BranchID, defBranch.ID)
		if resTable.TableID == table.TableID {
			tableFound = true
		}
	}
	assert.True(t, tableFound)

	// Get table (without table and columns metadata)
	respGet1, err := api.GetTableRequest(tableKey).Send(ctx)
	assert.NoError(t, err)
	removeDynamicValueFromTable(respGet1)
	assert.Equal(t, &Table{
		TableKey:      tableKey,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableKey.TableID.String(),
		Name:          tableKey.TableID.TableName,
		DisplayName:   tableKey.TableID.TableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
			BucketKey:   bucket.BucketKey,
			DisplayName: bucket.DisplayName,
			URI:         "https://" + project.StorageAPIHost() + "/v2/storage/buckets/" + tableKey.BucketKey().BucketID.String(),
		},
		Metadata:       TableMetadata{},
		ColumnMetadata: ColumnsMetadata{},
	}, respGet1)

	// Set metadata
	resMetadata, err := api.
		CreateOrUpdateTableMetadata(
			tableKey,
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
	respGet2, err := api.GetTableRequest(tableKey).Send(ctx)
	assert.NoError(t, err)
	removeDynamicValuesFromTableMetadata(respGet2.Metadata)
	removeDynamicValuesFromColumnsMetadata(respGet2.ColumnMetadata)
	removeDynamicValueFromTable(respGet2)
	assert.Equal(t, &Table{
		TableKey:      tableKey,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableKey.TableID.String(),
		Name:          tableKey.TableID.TableName,
		DisplayName:   tableKey.TableID.TableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
			BucketKey:   bucket.BucketKey,
			DisplayName: bucket.DisplayName,
			URI:         "https://" + project.StorageAPIHost() + "/v2/storage/buckets/" + tableKey.BucketKey().BucketID.String(),
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

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.FileID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableKey, file1.FileKey, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Create file
	fileName2 := fmt.Sprintf("file_%d", rnd.Int())
	file2, err := api.CreateFileResourceRequest(defBranch.ID, fileName2).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file2.FileID)

	// Check rows count
	table, err := api.GetTableRequest(tableKey).Send(ctx)
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
	job, err := api.LoadDataFromFileRequest(tableKey, file2.FileKey, WithColumnsHeaders([]string{"col2", "col1"}), WithIncrementalLoad(true)).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx2, job))

	// Check rows count
	table, err = api.GetTableRequest(tableKey).Send(ctx)
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

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	_, err = api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, bucket.BucketID)

	// Create whole file
	wholeFile, err := api.CreateFileResourceRequest(defBranch.ID, "file name").Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, wholeFile.FileID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, wholeFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create non-sliced table.
	// Table cannot be created from a sliced file (https://keboola.atlassian.net/browse/KBC-1861).
	_, err = api.CreateTableFromFileRequest(tableKey, wholeFile.FileKey, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(tableKey).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)

	// Create sliced file
	slicedFile, err := api.CreateFileResourceRequest(defBranch.ID, "file name", WithIsSliced(true)).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, slicedFile.FileID)

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
	job, err := api.LoadDataFromFileRequest(tableKey, slicedFile.FileKey, WithIncrementalLoad(true), WithColumnsHeaders([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx, job))

	// Check rows count
	table, err = api.GetTableRequest(tableKey).Send(ctx)
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

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.FileID)

	// Upload file
	content := []byte("'col1'&'col2'\n'val1'&'val2'\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableKey, file1.FileKey, WithDelimiter("&"), WithEnclosure("'")).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(tableKey).Send(ctx)
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

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	inputFile, err := api.CreateFileResourceRequest(defBranch.ID, fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, inputFile.FileID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, inputFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableKey, inputFile.FileKey, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Unload table as CSV
	outputFileInfo, err := api.NewTableUnloadRequest(tableKey).
		WithColumns("col1").
		WithChangedSince("-2 days").
		WithFormat(UnloadFormatCSV).
		WithLimitRows(100).
		WithOrderBy("col1", OrderAsc).
		WithWhere("col1", CompareEq, []string{"val1"}).
		SendAndWait(ctx, time.Minute*5)
	assert.NoError(t, err)

	// Download file
	credentials, err := api.GetFileWithCredentialsRequest(outputFileInfo.File.FileKey).Send(ctx)
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

func TestCreateTableDefinition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, api := APIClientForAnEmptyProject(t, ctx)

	// Get default branch
	defBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	require.NoError(t, err)

	bucket, tableKey := getBucketAndTableKey(defBranch)

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// min use-case Create Table
	requestPayload := &CreateTableRequest{
		Name: tableKey.TableID.TableName,
		TableDefinition: TableDefinition{
			PrimaryKeyNames: []string{"name"},
			Columns: []Column{
				{
					Name:       "name",
					BaseType:   TypeString,
					Definition: ColumnDefinition{Type: "STRING"},
				},
				{
					Name:       "age",
					BaseType:   TypeNumeric,
					Definition: ColumnDefinition{Type: "INT"},
				},
				{
					Name:       "time",
					BaseType:   TypeDate,
					Definition: ColumnDefinition{Type: "DATE"},
				},
			},
		},
	}

	// Create a new table
	newTable, err := api.CreateTableDefinitionRequest(tableKey, requestPayload).Send(ctx)
	require.NoError(t, err)
	assert.Equal(t, requestPayload.Name, newTable.Name)

	for _, column := range newTable.Definition.Columns {
		for _, primaryKey := range newTable.PrimaryKey {
			if column.Name == primaryKey {
				assert.False(t, column.Definition.Nullable)
			}
		}
	}

	// Get a list of the tables
	resTables, err := api.ListTablesRequest(defBranch.ID).Send(ctx)
	require.NoError(t, err)

	tableFound := false
	for _, table := range *resTables {
		if table.TableID == tableKey.TableID {
			tableFound = true
		}
	}
	assert.True(t, tableFound)

	// Get a specific table by tableID
	resTab, err := api.GetTableRequest(newTable.TableKey).Send(ctx)
	removeDynamicValueFromTable(resTab)
	resTab.Metadata = TableMetadata{}
	resTab.ColumnMetadata = ColumnsMetadata{}
	require.NoError(t, err)

	assert.Equal(t, &Table{
		TableKey:    newTable.TableKey,
		URI:         newTable.URI,
		Name:        newTable.Name,
		DisplayName: newTable.DisplayName,
		SourceTable: nil,
		PrimaryKey:  newTable.PrimaryKey,
		Definition: &TableDefinition{
			PrimaryKeyNames: requestPayload.PrimaryKeyNames,
			Columns: []Column{
				{
					Name:       "age",
					BaseType:   TypeNumeric,
					Definition: ColumnDefinition{Type: "NUMBER", Length: DefaultNumber, Nullable: true},
				},
				{
					Name:       "name",
					BaseType:   TypeString,
					Definition: ColumnDefinition{Type: "VARCHAR", Length: DefaultString, Nullable: false},
				},
				{
					Name:       "time",
					BaseType:   TypeDate,
					Definition: ColumnDefinition{Type: "DATE", Nullable: true},
				},
			},
		},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        newTable.Columns,
		Metadata:       TableMetadata{},
		ColumnMetadata: ColumnsMetadata{},
		Bucket: &Bucket{
			BucketKey:   bucket.BucketKey,
			DisplayName: bucket.DisplayName,
			URI:         "https://" + project.StorageAPIHost() + "/v2/storage/buckets/" + tableKey.TableID.BucketID.String(),
		},
	}, resTab)
	assert.Equal(t, requestPayload.Name, resTab.Name)
	assert.Equal(t, len(newTable.Columns), len(resTab.Columns))

	// Delete the table that was created in the CreateTableDefinitionRequest func
	_, err = api.DeleteTableRequest(defBranch.ID, newTable.TableID).Send(ctx)
	require.NoError(t, err)

	// Get a list of the tables
	res, err := api.ListTablesRequest(defBranch.ID).Send(ctx)
	require.NoError(t, err)
	assert.Empty(t, res)

	// max-use CreateTable
	maxUseCaseRequest := &CreateTableRequest{
		TableDefinition: TableDefinition{
			PrimaryKeyNames: []string{"email"},
			Columns: []Column{
				{
					Name: "email",
					Definition: ColumnDefinition{
						Type:     "VARCHAR",
						Length:   DefaultString,
						Nullable: false,
						Default:  "",
					},
					BaseType: TypeString,
				},
				{
					Name: "comments",
					Definition: ColumnDefinition{
						Type:    "NUMBER",
						Length:  "37",
						Default: "100",
					},
					BaseType: TypeNumeric,
				},
				{
					Name: "favorite_number",
					Definition: ColumnDefinition{
						Type:     "NUMBER",
						Length:   "37",
						Nullable: true,
						Default:  "100",
					},
					BaseType: TypeNumeric,
				},
			},
		},
		Name: "MaxUseCase",
	}

	// Create Table
	minimumUseCase, err := api.CreateTableDefinitionRequest(tableKey, maxUseCaseRequest).Send(ctx)
	require.NoError(t, err)

	maxUseCaseTable, err := api.GetTableRequest(minimumUseCase.TableKey).Send(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Column{
		{
			Name: "comments",
			Definition: ColumnDefinition{
				Type:     "NUMBER",
				Length:   "37,0",
				Nullable: true,
				Default:  "100",
			},
			BaseType: TypeNumeric,
		},
		{
			Name: "email",
			Definition: ColumnDefinition{
				Type:     "VARCHAR",
				Length:   DefaultString,
				Nullable: false,
			},
			BaseType: TypeString,
		},
		{
			Name: "favorite_number",
			Definition: ColumnDefinition{
				Type:     "NUMBER",
				Length:   "37,0",
				Nullable: true,
				Default:  "100",
			},
			BaseType: TypeNumeric,
		},
	}, maxUseCaseTable.Definition.Columns)

	// Delete the table that was created in the CreateTableDefinitionRequest func
	_, err = api.DeleteTableRequest(defBranch.ID, maxUseCaseTable.TableID).Send(ctx)
	require.NoError(t, err)
}

func removeDynamicValueFromTable(table *Table) {
	table.Created = iso8601.Time{}
	table.LastImportDate = iso8601.Time{}
	table.LastChangeDate = nil
	table.Bucket.Created = iso8601.Time{}
	table.Bucket.LastChangeDate = nil
}

func getBucketAndTableKey(branch *Branch) (*Bucket, TableKey) {
	bucket := &Bucket{
		BucketKey: BucketKey{
			BranchID: branch.ID,
			BucketID: BucketID{
				Stage:      BucketStageIn,
				BucketName: fmt.Sprintf("c-test_%d", rnd.Int()),
			},
		},
	}

	tableKey := TableKey{
		BranchID: branch.ID,
		TableID: TableID{
			BucketID:  bucket.BucketID,
			TableName: fmt.Sprintf("test_%d", rnd.Int()),
		},
	}
	return bucket, tableKey
}
