package storageapi

type BucketID string

func (v BucketID) String() string {
	return string(v)
}

type BucketKey struct {
	BranchID BranchID `json:"branchId"`
	ID       BucketID `json:"id"`
}

func (k BucketKey) ObjectId() any {
	return k.ID
}

// "uri": "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-8103767",
// "id": "in.c-keboola-ex-http-8103767",
// "name": "c-keboola-ex-http-8103767",
// "displayName": "keboola-ex-http-8103767",
// "stage": "in",
// "description": "",
// "tables": "https://connection.north-europe.azure.keboola.com/v2/storage/buckets/in.c-keboola-ex-http-8103767",
// "created": "2021-11-30T16:48:11+0100",
// "lastChangeDate": "2022-03-18T13:45:42+0100",
// "isReadOnly": false,
// "dataSizeBytes": 3072,
// "rowsCount": 8,
// "isMaintenance": false,
// "backend": "snowflake",
// "sharing": null,
// "hasExternalSchema": false,
// "databaseName": ""

type Bucket struct {
	BucketKey
	Uri            string `json:"uri"`
	Name           string `json:"name"`
	DisplayName    string `json:"displayName"`
	Stage          string `json:"stage"`
	Description    string `json:"description"`
	Created        string `json:"created"`
	LastChangeDate string `json:"lastChangeDate"`
	IsReadOnly     bool   `json:"isReadOnly"`
	DataSizeBytes  int    `json:"dataSizeBytes"`
	RowsCount      int    `json:"rowsCount"`
}
