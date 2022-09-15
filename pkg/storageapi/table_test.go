package storageapi_test

import (
	"context"
	"fmt"
	"testing"

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
		result, err := ListTablesRequest(branchKey).Send(ctx, c)
		fmt.Println(result)
		assert.NoError(t, err)
		assert.Len(t, *result, 1)
	}

	{
		result, err := ListTablesRequest(branchKey, WithBuckets(), WithMetadata()).Send(ctx, c)
		assert.NoError(t, err)
		assert.Len(t, *result, 1)
		assert.NotNil(t, (*result)[0].Bucket)
		assert.NotNil(t, (*result)[0].Metadata)
		assert.Len(t, (*result)[0].Metadata, 3)
	}
}
