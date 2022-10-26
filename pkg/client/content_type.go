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
	if contentType == ContentTypeApplicationJson {
		return true
	}
	return jsonContentTypeRegexp.MatchString(contentType)
}
