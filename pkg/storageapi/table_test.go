package storageapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func newJsonResponder(status int, response string) httpmock.Responder {
	r := httpmock.NewStringResponse(status, response)
	r.Header.Set("Content-Type", "application/json")
	return httpmock.ResponderFromResponse(r)
}

func parseDate(value string) Time {
	t, err := time.Parse(TimeFormat, value)
	if err != nil {
		panic(err)
	}
	return Time(t)
}

func listTablesMock() client.Client {
	c, transport := client.NewMockedClient()
	transport.RegisterResponder(
		"GET",
		"v2/storage/branch/0/tables?include=",
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
		"v2/storage/branch/0/tables?include=buckets%2Cmetadata",
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
	c := listTablesMock()
	branchKey := BranchKey{ID: 0}

	{
		expected := &[]*Table{
			{
				TableKey: TableKey{
					BranchID: branchKey.ID,
					ID:       "in.c-keboola-ex-http-6336016.tmp1",
				},
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     []string{},
				Created:        parseDate("2021-10-15T13:38:11+0200"),
				LastImportDate: parseDate("2021-10-15T13:41:59+0200"),
				LastChangeDate: parseDate("2021-10-15T13:41:59+0200"),
				RowsCount:      6,
				DataSizeBytes:  1536,
				Columns:        nil,
				Metadata:       nil,
				ColumnMetadata: nil,
				Bucket:         nil,
			},
		}

		actual, err := ListTablesRequest(branchKey).Send(ctx, c)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	{
		expected := &[]*Table{
			{
				TableKey: TableKey{
					BranchID: branchKey.ID,
					ID:       "in.c-keboola-ex-http-6336016.tmp1",
				},
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     []string{},
				Created:        parseDate("2021-10-15T13:38:11+0200"),
				LastImportDate: parseDate("2021-10-15T13:41:59+0200"),
				LastChangeDate: parseDate("2021-10-15T13:41:59+0200"),
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
					BucketKey: BucketKey{
						BranchID: branchKey.ID,
						ID:       "in.c-keboola-ex-http-6336016",
					},
					Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					Name:           "c-keboola-ex-http-6336016",
					DisplayName:    "keboola-ex-http-6336016",
					Stage:          BucketStageIn,
					Description:    "",
					Created:        parseDate("2021-10-15T11:29:09+0200"),
					LastChangeDate: parseDate("2022-02-15T16:50:49+0100"),
					IsReadOnly:     false,
					DataSizeBytes:  1536,
					RowsCount:      6,
				},
			},
		}

		actual, err := ListTablesRequest(branchKey, WithBuckets(), WithMetadata()).Send(ctx, c)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}