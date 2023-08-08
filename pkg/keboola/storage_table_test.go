package keboola_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/keboola"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

func newJSONResponder(response string) httpmock.Responder {
	r := httpmock.NewStringResponse(200, response)
	r.Header.Set("Content-Type", "application/json")
	return httpmock.ResponderFromResponse(r)
}

func parseDate(value string) iso8601.Time {
	t, err := iso8601.ParseString(value)
	if err != nil {
		panic(err)
	}
	return iso8601.Time{
		Time: t,
	}
}

func mockedListTablesClient() client.Client {
	c, transport := client.NewMockedClient()
	transport.RegisterResponder("GET", `https://connection.north-europe.azure.keboola.com/v2/storage/?exclude=components`, newJSONResponder(`{
		"services": [],
		"features": []
	}`))
	transport.RegisterResponder(
		"GET",
		"https://connection.north-europe.azure.keboola.com/v2/storage/tables?include=",
		newJSONResponder(`[
			{
				"uri": "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				"id": "in.c-keboola-ex-http-6336016.tmp1",
				"name": "tmp1",
				"displayName": "tmp1",
				"transactional": false,
				"primaryKey": [],
				"indexType": null,
				"indexKey": [],
				"distributionType": null,
				"distributionKey": [],
				"syntheticPrimaryKeyEnabled": false,
				"created": "2021-10-15T13:38:11+0200",
				"lastImportDate": "2021-10-15T13:41:59+0200",
				"lastChangeDate": "2021-10-15T13:41:59+0200",
				"rowsCount": 6,
				"dataSizeBytes": 1536,
				"isAlias": false,
				"isAliasable": true,
				"isTyped": false
			}
		]`),
	)
	transport.RegisterResponder(
		"GET",
		"https://connection.north-europe.azure.keboola.com/v2/storage/tables?include=buckets%2Cmetadata",
		newJSONResponder(`[
			{
				"uri": "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				"id": "in.c-keboola-ex-http-6336016.tmp1",
				"name": "tmp1",
				"displayName": "tmp1",
				"transactional": false,
				"primaryKey": [],
				"indexType": null,
				"indexKey": [],
				"distributionType": null,
				"distributionKey": [],
				"syntheticPrimaryKeyEnabled": false,
				"created": "2021-10-15T13:38:11+0200",
				"lastImportDate": "2021-10-15T13:41:59+0200",
				"lastChangeDate": "2021-10-15T13:41:59+0200",
				"rowsCount": 6,
				"dataSizeBytes": 1536,
				"isAlias": false,
				"isAliasable": true,
				"isTyped": false,
				"bucket": {
					"uri": "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					"id": "in.c-keboola-ex-http-6336016",
					"name": "c-keboola-ex-http-6336016",
					"displayName": "keboola-ex-http-6336016",
					"stage": "in",
					"description": "",
					"tables": "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					"created": "2021-10-15T11:29:09+0200",
					"lastChangeDate": "2022-02-15T16:50:49+0100",
					"isReadOnly": false,
					"dataSizeBytes": 1536,
					"rowsCount": 6,
					"isMaintenance": false,
					"backend": "snowflake",
					"sharing": null,
					"hasExternalSchema": false,
					"databaseName": ""
				},
				"metadata": [
					{
						"id": "73234506",
						"key": "KBC.lastUpdatedBy.component.id",
						"value": "keboola.ex-http",
						"provider": "system",
						"timestamp": "2021-10-15T13:42:30+0200"
					},
					{
						"id": "73234507",
						"key": "KBC.lastUpdatedBy.configuration.id",
						"value": "6336016",
						"provider": "system",
						"timestamp": "2021-10-15T13:42:30+0200"
					},
					{
						"id": "73234508",
						"key": "KBC.lastUpdatedBy.configurationRow.id",
						"value": "6336185",
						"provider": "system",
						"timestamp": "2021-10-15T13:42:30+0200"
					}
				]
			}
		]`),
	)
	return c
}

func TestListTablesRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	tables, err := api.ListTablesRequest().Send(ctx)
	assert.NoError(t, err)
	assert.Len(t, *tables, 0)
}

func TestMockListTablesRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := mockedListTablesClient()
	api, err := NewAPI(ctx, "https://connection.north-europe.azure.keboola.com", WithClient(&c))
	assert.NoError(t, err)
	{
		lastChangedDate := parseDate("2021-10-15T13:41:59+0200")
		expected := &[]*Table{
			{
				ID:             MustParseTableID("in.c-keboola-ex-http-6336016.tmp1"),
				URI:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     []string{},
				Created:        parseDate("2021-10-15T13:38:11+0200"),
				LastImportDate: parseDate("2021-10-15T13:41:59+0200"),
				LastChangeDate: &lastChangedDate,
				RowsCount:      6,
				DataSizeBytes:  1536,
				Columns:        nil,
				Metadata:       nil,
				ColumnMetadata: nil,
				Bucket:         nil,
			},
		}

		actual, err := api.ListTablesRequest().Send(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	{
		lastChangeDate := parseDate("2022-02-15T16:50:49+0100")
		lastChangeDateTable := parseDate("2021-10-15T13:41:59+0200")
		expected := &[]*Table{
			{
				ID:             MustParseTableID("in.c-keboola-ex-http-6336016.tmp1"),
				URI:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     []string{},
				Created:        parseDate("2021-10-15T13:38:11+0200"),
				LastImportDate: parseDate("2021-10-15T13:41:59+0200"),
				LastChangeDate: &lastChangeDateTable,
				RowsCount:      6,
				DataSizeBytes:  1536,
				Columns:        nil,
				Metadata: []MetadataDetail{
					{
						ID:        "73234506",
						Key:       "KBC.lastUpdatedBy.component.id",
						Value:     "keboola.ex-http",
						Timestamp: "2021-10-15T13:42:30+0200",
						Provider:  "system",
					},
					{
						ID:        "73234507",
						Key:       "KBC.lastUpdatedBy.configuration.id",
						Value:     "6336016",
						Timestamp: "2021-10-15T13:42:30+0200",
						Provider:  "system",
					},
					{
						ID:        "73234508",
						Key:       "KBC.lastUpdatedBy.configurationRow.id",
						Value:     "6336185",
						Timestamp: "2021-10-15T13:42:30+0200",
						Provider:  "system",
					},
				},
				ColumnMetadata: nil,
				Bucket: &Bucket{
					BucketID:       MustParseBucketID("in.c-keboola-ex-http-6336016"),
					URI:            "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					DisplayName:    "keboola-ex-http-6336016",
					Description:    "",
					Created:        parseDate("2021-10-15T11:29:09+0200"),
					LastChangeDate: &lastChangeDate,
					IsReadOnly:     false,
					DataSizeBytes:  1536,
					RowsCount:      6,
				},
			},
		}

		actual, err := api.ListTablesRequest(WithBuckets(), WithMetadata()).Send(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

func TestTableApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	project, api := APIClientForAnEmptyProject(t, ctx)

	bucketName := fmt.Sprintf("c-test_%d", rnd.Int())
	tableName := fmt.Sprintf("test_%d", rnd.Int())

	bucket := &Bucket{
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
		ID:         tableID,
		Bucket:     bucket,
		Name:       tableName,
		Columns:    []string{"first", "second", "third", "fourth"},
		PrimaryKey: []string{"first", "fourth"},
	}

	// Create table
	res, err := api.CreateTableRequest(tableID, table.Columns).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, table.Name, res.Name)

	// List tables
	resList, err := api.ListTablesRequest().Send(ctx)
	assert.NoError(t, err)
	tableFound := false
	for _, t := range *resList {
		if t.ID == table.ID {
			tableFound = true
		}
	}
	assert.True(t, tableFound)

	// Get table (without table and columns metadata)
	respGet1, err := api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	respGet1.Created = iso8601.Time{}
	respGet1.LastImportDate = iso8601.Time{}
	respGet1.LastChangeDate = nil
	respGet1.Bucket.Created = iso8601.Time{}
	respGet1.Bucket.LastChangeDate = nil
	assert.Equal(t, &Table{
		ID:            tableID,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableID.String(),
		Name:          tableName,
		DisplayName:   tableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
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
	respGet2, err := api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	removeDynamicValuesFromTableMetadata(respGet2.Metadata)
	removeDynamicValuesFromColumnsMetadata(respGet2.ColumnMetadata)
	respGet2.Created = iso8601.Time{}
	respGet2.LastImportDate = iso8601.Time{}
	respGet2.LastChangeDate = nil
	respGet2.Bucket.Created = iso8601.Time{}
	respGet2.Bucket.LastChangeDate = nil
	assert.Equal(t, &Table{
		ID:            tableID,
		URI:           "https://" + project.StorageAPIHost() + "/v2/storage/tables/" + tableID.String(),
		Name:          tableName,
		DisplayName:   tableName,
		PrimaryKey:    []string{},
		RowsCount:     0,
		DataSizeBytes: 0,
		Columns:       []string{"first", "second", "third", "fourth"},
		Bucket: &Bucket{
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
	_, err = api.DeleteTableRequest(table.ID, WithForce()).Send(ctx)
	assert.NoError(t, err)

	// List tables again - without the deleted table
	resList, err = api.ListTablesRequest().Send(ctx)
	assert.NoError(t, err)
	tableFound = false
	for _, t := range *resList {
		if t.ID == table.ID {
			tableFound = true
		}
	}
	assert.False(t, tableFound)
}

func TestTableCreateLoadDataFromFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableID, file1.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Create file
	fileName2 := fmt.Sprintf("file_%d", rnd.Int())
	file2, err := api.CreateFileResourceRequest(fileName2).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file2.ID)

	// Check rows count
	table, err := api.GetTableRequest(tableID).Send(ctx)
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
	job, err := api.LoadDataFromFileRequest(tableID, file2.ID, WithColumnsHeaders([]string{"col2", "col1"}), WithIncrementalLoad(true)).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx2, job))

	// Check rows count
	table, err = api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), table.RowsCount)
}

func TestTableCreateFromSlicedFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx, testproject.WithStagingStorageS3())

	bucketName := fmt.Sprintf("c-test_%d", rnd.Int())
	tableName := fmt.Sprintf("test_%d", rnd.Int())

	bucket := &Bucket{
		BucketID: BucketID{
			Stage:      BucketStageIn,
			BucketName: bucketName,
		},
	}

	// Create bucket
	_, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, bucket.BucketID)
	tableID := TableID{
		BucketID:  bucket.BucketID,
		TableName: tableName,
	}

	// Create whole file
	wholeFile, err := api.CreateFileResourceRequest(tableName).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, wholeFile.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, wholeFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create non-sliced table.
	// Table cannot be created from a sliced file (https://keboola.atlassian.net/browse/KBC-1861).
	_, err = api.CreateTableFromFileRequest(tableID, wholeFile.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)

	// Create sliced file
	slicedFile, err := api.CreateFileResourceRequest(tableName, WithIsSliced(true)).Send(ctx)
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
	job, err := api.LoadDataFromFileRequest(tableID, slicedFile.ID, WithIncrementalLoad(true), WithColumnsHeaders([]string{"col1", "col2"})).Send(ctx)
	assert.NoError(t, err)
	assert.NoError(t, api.WaitForStorageJob(waitCtx, job))

	// Check rows count
	table, err = api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), table.RowsCount)
}

func TestTableCreateFromFileOtherOptions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	file1, err := api.CreateFileResourceRequest(fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, file1.ID)

	// Upload file
	content := []byte("'col1'&'col2'\n'val1'&'val2'\n")
	written, err := Upload(ctx, file1, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableID, file1.ID, WithDelimiter("&"), WithEnclosure("'")).Send(ctx)
	assert.NoError(t, err)

	// Check rows count
	table, err := api.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)
}

func TestTableUnloadRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForAnEmptyProject(t, ctx)

	bucketID := BucketID{
		Stage:      BucketStageIn,
		BucketName: fmt.Sprintf("c-bucket_%d", rnd.Int()),
	}
	tableID := TableID{
		BucketID:  bucketID,
		TableName: fmt.Sprintf("table_%d", rnd.Int()),
	}
	bucket := &Bucket{
		BucketID: bucketID,
	}

	// Create bucket
	resBucket, err := api.CreateBucketRequest(bucket).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	fileName1 := fmt.Sprintf("file_%d", rnd.Int())
	inputFile, err := api.CreateFileResourceRequest(fileName1).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, inputFile.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, inputFile, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	_, err = api.CreateTableFromFileRequest(tableID, inputFile.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx)
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
	credentials, err := api.GetFileWithCredentialsRequest(outputFileInfo.File.ID).Send(ctx)
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
