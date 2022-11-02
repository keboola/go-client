package abs

import (
	"github.com/relvacode/iso8601"
)

//nolint:tagliatelle
type UploadParamsCredentials struct {
	SASConnectionString string       `json:"SASConnectionString"`
	Expiration          iso8601.Time `json:"expiration"`
}

type UploadParams struct {
	BlobName    string                  `json:"blobName"`
	AccountName string                  `json:"accountName"`
	Container   string                  `json:"container"`
	Credentials UploadParamsCredentials `json:"absCredentials"`
}
