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

const (
	BucketStageIn  = "in"
	BucketStageOut = "out"
)

type Bucket struct {
	BucketKey
	Uri            string `json:"uri"`
	Name           string `json:"name"`
	DisplayName    string `json:"displayName"`
	Stage          string `json:"stage"`
	Description    string `json:"description"`
	Created        Time   `json:"created"`
	LastChangeDate Time   `json:"lastChangeDate"`
	IsReadOnly     bool   `json:"isReadOnly"`
	DataSizeBytes  int    `json:"dataSizeBytes"`
	RowsCount      int    `json:"rowsCount"`
}
