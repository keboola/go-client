package client

import (
	"regexp"
)

const (
	ContentTypeApplicationJSON       = "application/json"
	ContentTypeApplicationJSONRegexp = `^application/([a-zA-Z0-9\.\-]+\+)?json$`
)

var jsonContentTypeRegexp = regexp.MustCompile(ContentTypeApplicationJSONRegexp)

func isJSONContentType(contentType string) bool {
	return jsonContentTypeRegexp.MatchString(contentType)
}
