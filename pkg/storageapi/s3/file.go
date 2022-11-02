package s3

import (
	"github.com/relvacode/iso8601"
)

//nolint:tagliatelle
type UploadParamsCredentials struct {
	AccessKeyId     string       `json:"AccessKeyId"`
	SecretAccessKey string       `json:"SecretAccessKey"`
	SessionToken    string       `json:"SessionToken"`
	Expiration      iso8601.Time `json:"Expiration"`
}

//nolint:tagliatelle
type UploadParams struct {
	Key         string                  `json:"key"`
	Bucket      string                  `json:"bucket"`
	Acl         string                  `json:"acl"`
	Credentials UploadParamsCredentials `json:"credentials"`
}
