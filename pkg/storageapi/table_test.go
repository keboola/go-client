package storageapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/storageapi"
)

func mockClient() client.Client {
	c, transport := client.NewMockedClient()
	transport.RegisterResponder(
		"GET",
		"v2/storage/branch/0/tables?include=",
		httpmock.NewJsonResponderOrPanic(200, []*Table{
			{
				TableKey: TableKey{
					BranchID: 0,
					ID:       "in.c-keboola-ex-http-6336016.tmp1",
				},
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     make([]string, 0),
				Created:        "2021-10-15T13:38:11+0200",
				LastImportDate: "2021-10-15T13:41:59+0200",
				LastChangeDate: "2021-10-15T13:41:59+0200",
				RowsCount:      6,
				DataSizeBytes:  1536,
			},
		}),
	)
	transport.RegisterResponder(
		"GET",
		"v2/storage/branch/0/tables?include=buckets%2Cmetadata",
		httpmock.NewJsonResponderOrPanic(200, []*Table{
			{
				TableKey: TableKey{
					BranchID: 0,
					ID:       "in.c-keboola-ex-http-6336016.tmp1",
				},
				Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/tables/in.c-keboola-ex-http-6336016.tmp1",
				Name:           "tmp1",
				DisplayName:    "tmp1",
				PrimaryKey:     make([]string, 0),
				Created:        "2021-10-15T13:38:11+0200",
				LastImportDate: "2021-10-15T13:41:59+0200",
				LastChangeDate: "2021-10-15T13:41:59+0200",
				RowsCount:      6,
				DataSizeBytes:  1536,
				Bucket: &Bucket{
					BucketKey: BucketKey{
						BranchID: 0,
						ID:       "in.c-keboola-ex-http-6336016",
					},
					Uri:            "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-6336016",
					Name:           "c-keboola-ex-http-6336016",
					DisplayName:    "keboola-ex-http-6336016",
					Stage:          "in",
					Description:    "",
					Created:        "2021-10-15T11:29:09+0200",
					LastChangeDate: "2022-02-15T16:50:49+0100",
					IsReadOnly:     false,
					DataSizeBytes:  1536,
					RowsCount:      6,
				},
				Metadata: &[]MetadataDetail{
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
			},
		}),
	)
	return c
}

func TestListTablesRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := mockClient()

	branchKey := BranchKey{ID: 0}

	{
		result, err := ListTablesRequest(branchKey).Send(ctx, c)
		assert.NoError(t, err)
		assert.Len(t, *result, 1)
		assert.Equal(t, (*result)[0].BranchID, branchKey.ID)
	}

	{
		result, err := ListTablesRequest(branchKey, WithBuckets(), WithMetadata()).Send(ctx, c)
		assert.NoError(t, err)
		assert.Len(t, *result, 1)
		assert.NotNil(t, (*result)[0].Bucket)
		assert.Equal(t, (*result)[0].Bucket.BranchID, branchKey.ID)
		assert.NotNil(t, (*result)[0].Metadata)
		assert.Len(t, (*(*result)[0].Metadata), 3)
	}
}
