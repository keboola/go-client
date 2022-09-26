package storageapi

type BucketID string

func (v BucketID) String() string {
	return string(v)
}

const (
	BucketStageIn  = "in"
	BucketStageOut = "out"
)

type Bucket struct {
	ID             BucketID `json:"id"`
	Uri            string   `json:"uri"`
	Name           string   `json:"name"`
	DisplayName    string   `json:"displayName"`
	Stage          string   `json:"stage"`
	Description    string   `json:"description"`
	Created        Time     `json:"created"`
	LastChangeDate Time     `json:"lastChangeDate"`
	IsReadOnly     bool     `json:"isReadOnly"`
	DataSizeBytes  uint64   `json:"dataSizeBytes"`
	RowsCount      uint64   `json:"rowsCount"`
}
