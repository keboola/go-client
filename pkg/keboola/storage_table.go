package keboola

import (
	"github.com/relvacode/iso8601"
)

type TableKey struct {
	BranchID BranchID `json:"-"`
	TableID  TableID  `json:"id"`
}

// Table https://keboola.docs.apiary.io/#reference/tables/list-tables/list-all-tables
type Table struct {
	TableKey
	URI            string           `json:"uri"`
	Name           string           `json:"name"`
	DisplayName    string           `json:"displayName"`
	SourceTable    *SourceTable     `json:"sourceTable"`
	PrimaryKey     []string         `json:"primaryKey"`
	Created        iso8601.Time     `json:"created"`
	LastImportDate iso8601.Time     `json:"lastImportDate"`
	LastChangeDate *iso8601.Time    `json:"lastChangeDate"`
	Definition     *TableDefinition `json:"definition,omitempty"`
	RowsCount      uint64           `json:"rowsCount"`
	DataSizeBytes  uint64           `json:"dataSizeBytes"`
	Columns        []string         `json:"columns"`
	Metadata       TableMetadata    `json:"metadata"`
	ColumnMetadata ColumnsMetadata  `json:"columnMetadata"`
	Bucket         *Bucket          `json:"bucket"`
}

type SourceTable struct {
	ID      TableID       `json:"id"`
	URI     string        `json:"uri"`
	Name    string        `json:"name"`
	Project SourceProject `json:"project"`
}

type SourceProject struct {
	ID   ProjectID `json:"id"`
	Name string    `json:"name"`
}

type TableDefinition struct {
	PrimaryKeyNames []string `json:"primaryKeysNames"`
	Columns         []Column `json:"columns"`
}

func (v TableKey) BucketKey() BucketKey {
	return BucketKey{
		BranchID: v.BranchID,
		BucketID: v.TableID.BucketID,
	}
}
