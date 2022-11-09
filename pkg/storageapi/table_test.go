package storageapi_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func newJsonResponder(status int, response string) httpmock.Responder {
	r := httpmock.NewStringResponse(status, response)
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

func listTablesMock() client.Client {
	c, transport := client.NewMockedClient()
	transport.RegisterResponder(
		"GET",
		"v2/storage/tables?include=",
		newJsonResponder(200, `[
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
				"indexedColumns": [],
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
		"v2/storage/tables?include=buckets%2Cmetadata",
		newJsonResponder(200, `[
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
				"indexedColumns": [],
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
	c := ClientForAnEmptyProject(t)

	tables, err := ListTablesRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.Len(t, *tables, 0)
}

func TestMockListTablesRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := listTablesMock()

	{
		lastChangedDate := parseDate("2021-10-15T13:41:59+0200")
		expected := &[]*Table{
			{
				ID:             "in.c-keboola-ex-http-6336016.tmp1",
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
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

		actual, err := ListTablesRequest().Send(ctx, c)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	{
		lastChangeDate := parseDate("2022-02-15T16:50:49+0100")
		lastChangeDateTable := parseDate("2021-10-15T13:41:59+0200")
		expected := &[]*Table{
			{
				ID:             "in.c-keboola-ex-http-6336016.tmp1",
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
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
					},
					{
						ID:        "73234507",
						Key:       "KBC.lastUpdatedBy.configuration.id",
						Value:     "6336016",
						Timestamp: "2021-10-15T13:42:30+0200",
					},
					{
						ID:        "73234508",
						Key:       "KBC.lastUpdatedBy.configurationRow.id",
						Value:     "6336185",
						Timestamp: "2021-10-15T13:42:30+0200",
					},
				},
				ColumnMetadata: nil,
				Bucket: &Bucket{
					ID:             "in.c-keboola-ex-http-6336016",
					Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					Name:           "c-keboola-ex-http-6336016",
					DisplayName:    "keboola-ex-http-6336016",
					Stage:          BucketStageIn,
					Description:    "",
					Created:        parseDate("2021-10-15T11:29:09+0200"),
					LastChangeDate: &lastChangeDate,
					IsReadOnly:     false,
					DataSizeBytes:  1536,
					RowsCount:      6,
				},
			},
		}

		actual, err := ListTablesRequest(WithBuckets(), WithMetadata()).Send(ctx, c)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

func TestTableApiCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := ClientForAnEmptyProject(t)

	bucketName := fmt.Sprintf("test_%d", rand.Int())
	tableName := fmt.Sprintf("test_%d", rand.Int())

	bucket := &Bucket{
		Name:  bucketName,
		Stage: "in",
	}

	// Create bucket
	resBucket, err := CreateBucketRequest(bucket).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	table := &Table{
		ID:         TableID(fmt.Sprintf("%s.%s", bucket.ID, tableName)),
		Bucket:     bucket,
		Name:       tableName,
		Columns:    []string{"first", "second", "third", "fourth"},
		PrimaryKey: []string{"first", "fourth"},
	}

	// Create table
	resTable, err := CreateTableRequest(table).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, table, resTable)

	// List tables
	resList, err := ListTablesRequest().Send(ctx, c)
	assert.NoError(t, err)
	tableFound := false
	for _, t := range *resList {
		if t.ID == table.ID {
			tableFound = true
		}
	}
	assert.True(t, tableFound)

	// Delete table
	_, err = DeleteTableRequest(table.ID, WithForce()).Send(ctx, c)
	assert.NoError(t, err)

	// List tables again - without the deleted table
	resList, err = ListTablesRequest().Send(ctx, c)
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
	c := ClientForAnEmptyProject(t)

	bucketName := fmt.Sprintf("test_%d", rand.Int())
	tableName := fmt.Sprintf("test_%d", rand.Int())

	bucket := &Bucket{
		Name:  bucketName,
		Stage: "in",
	}

	// Create bucket
	resBucket, err := CreateBucketRequest(bucket).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, bucket, resBucket)

	// Create file
	file := &File{
		IsPublic:    false,
		IsPermanent: false,
		IsSliced:    false,
		IsEncrypted: false,
		Name:        tableName,
	}
	_, err = CreateFileResourceRequest(file).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, file.ID)

	// Upload file
	content := []byte("col1,col2\nval1,val2\n")
	written, err := Upload(ctx, file, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Create table
	job, err := CreateTableFromFileRequest(string(bucket.ID), tableName, file.ID, WithPrimaryKey([]string{"col1", "col2"})).Send(ctx, c)
	assert.NoError(t, err)
	assert.NoError(t, WaitForJob(ctx, c, job))
	tableID := TableID(fmt.Sprintf("%s.%s", bucket.ID, tableName))

	// Create file
	file = &File{
		IsPublic:    false,
		IsPermanent: false,
		IsSliced:    false,
		IsEncrypted: false,
		Name:        tableName,
	}
	_, err = CreateFileResourceRequest(file).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, file.ID)

	// Check rows count
	table, err := GetTableRequest(tableID).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), table.RowsCount)

	// Upload file
	content = []byte("val2,val3\nval3,val4\nval4,val5\n")
	written, err = Upload(ctx, file, bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)

	// Load data to table - added three rows
	job, err = LoadDataFromFileRequest(tableID, file.ID, WithColumnsHeaders([]string{"col2", "col1"}), WithIncrementalLoad(true)).Send(ctx, c)
	assert.NoError(t, err)
	assert.NoError(t, WaitForJob(ctx, c, job))

	// Check rows count
	table, err = GetTableRequest(tableID).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), table.RowsCount)
}
