package storageapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type TableID struct {
	BucketID
	TableName string `validate:"required,min=1,max=96"`
}

func (v TableID) String() string {
	return fmt.Sprintf("%s.%s", v.BucketID.String(), v.TableName)
}

func (v TableID) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v *TableID) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	tableID, err := ParseTableID(str)
	if err != nil {
		return err
	}
	*v = tableID
	return nil
}

func MustParseTableID(v string) TableID {
	val, err := ParseTableID(v)
	if err != nil {
		panic(err)
	}
	return val
}

func ParseTableID(v string) (TableID, error) {
	if len(v) == 0 {
		return TableID{}, errors.New(`table ID cannot be empty`)
	}

	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return TableID{}, fmt.Errorf(`invalid table ID "%s": unexpected number of fragments`, v)
	}

	bucketID, err := ParseBucketID(parts[0] + "." + parts[1])
	if err != nil {
		return TableID{}, err
	}

	tableName := parts[2]

	if len(tableName) == 0 {
		return TableID{}, fmt.Errorf(`invalid table ID "%s": table ID cannot be empty`, v)
	}

	return TableID{
		BucketID:  bucketID,
		TableName: tableName,
	}, nil
}
