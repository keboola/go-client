package keboola

import (
	"fmt"

	"github.com/relvacode/iso8601"
)

type TimePartitioningType string

const (
	Day   TimePartitioningType = "DAY"
	Hour  TimePartitioningType = "HOUR"
	Month TimePartitioningType = "MONTH"
	Year  TimePartitioningType = "YEAR"
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
	PrimaryKeyNames   []string           `json:"primaryKeysNames"`
	Columns           Columns            `json:"columns"`
	TimePartitioning  *TimePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *RangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *Clustering        `json:"clustering,omitempty"`
}

type TimePartitioning struct {
	Type         TimePartitioningType `json:"type"`
	ExpirationMs string               `json:"expirationMs,omitempty"`
	Field        string               `json:"field,omitempty"`
}

type RangePartitioning struct {
	Field string `json:"field"`
	Range Range  `json:"range"`
}

type Range struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Interval string `json:"interval"`
}

type Clustering struct {
	Fields []string `json:"fields"`
}

func (v TableKey) BucketKey() BucketKey {
	return BucketKey{
		BranchID: v.BranchID,
		BucketID: v.TableID.BucketID,
	}
}

func (t TimePartitioningType) String() string {
	return string(t)
}

func (v TableKey) String() string {
	return fmt.Sprintf("%s/%s", v.BranchID.String(), v.TableID.String())
}
