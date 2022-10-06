package client

import (
	jsoniter "github.com/json-iterator/go"
)

// json - replacement of the standard encoding/json library, it is faster for larger responses.
var json = jsoniter.ConfigCompatibleWithStandardLibrary //nolint:gochecknoglobals
