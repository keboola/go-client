package decode

import (
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/andybalholm/brotli"
)

func Decode(body io.ReadCloser, contentEncoding string) (io.ReadCloser, error) {
	contentEncoding = strings.ToLower(contentEncoding)
	switch contentEncoding {
	case "gzip":
		if v, err := gzip.NewReader(body); err == nil {
			return v, nil
		} else {
			return nil, fmt.Errorf("cannot decode gzip: %w", err)
		}
	case "br":
		return io.NopCloser(brotli.NewReader(body)), nil
	default:
		return body, nil
	}
}
