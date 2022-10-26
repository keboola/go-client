package client

import (
	"regexp"
)

const (
	ContentTypeApplicationJson       = "application/json"
	ContentTypeApplicationJsonRegexp = `^application/([a-zA-Z0-9\.\-]+\+)?json$`
)

var jsonContentTypeRegexp = regexp.MustCompile(ContentTypeApplicationJsonRegexp)

func isJsonContentType(contentType string) bool {
	return jsonContentTypeRegexp.MatchString(contentType)
}
