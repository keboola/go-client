package keboola

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const magicBucketNamePrefix = "c-"

const (
	BucketStageIn  = "in"
	BucketStageOut = "out"
	BucketStageSys = "sys"
)

// nolint:gochecknoglobals
var stagesMap = map[string]bool{
	BucketStageIn:  true,
	BucketStageOut: true,
	BucketStageSys: true,
}

type BucketID struct {
	Stage      string `validate:"required,oneof=in out sys"`
	BucketName string `validate:"required,min=1,max=96"`
}

func (v BucketID) String() string {
	return fmt.Sprintf("%s.%s", v.Stage, v.BucketName)
}

func (v BucketID) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v *BucketID) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	bucketID, err := ParseBucketID(str)
	if err != nil {
		return err
	}
	*v = bucketID
	return nil
}

func MustParseBucketID(v string) BucketID {
	val, err := ParseBucketID(v)
	if err != nil {
		panic(err)
	}
	return val
}

func ParseBucketID(v string) (BucketID, error) {
	return parseBucketID(v, false)
}

func ParseBucketIDExpectMagicPrefix(v string) (BucketID, error) {
	return parseBucketID(v, true)
}

func parseBucketID(v string, magicPrefix bool) (BucketID, error) {
	if len(v) == 0 {
		return BucketID{}, errors.New(`bucket ID cannot be empty`)
	}

	parts := strings.Split(v, ".")
	if len(parts) != 2 {
		return BucketID{}, fmt.Errorf(`invalid bucket ID "%s": unexpected number of fragments`, v)
	}

	stage, bucket := parts[0], parts[1]

	if !stagesMap[stage] {
		return BucketID{}, fmt.Errorf(`invalid bucket ID "%s": unexpected stage "%s"`, v, stage)
	}

	if magicPrefix {
		if !strings.HasPrefix(bucket, magicBucketNamePrefix) {
			return BucketID{}, fmt.Errorf(`invalid bucket ID "%s": missing expected prefix "c-"`, v)
		}
	}

	if len(bucket) == 0 || bucket == magicBucketNamePrefix {
		return BucketID{}, fmt.Errorf(`invalid bucket ID "%s": bucket ID cannot be empty`, v)
	}

	return BucketID{
		Stage:      stage,
		BucketName: bucket,
	}, nil
}
