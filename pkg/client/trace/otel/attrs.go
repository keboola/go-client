package otel

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv/v1.18.0/httpconv"

	"github.com/keboola/go-client/pkg/request"
)

const (
	maskedAttrValue = "****"
)

type attributes struct {
	config config
	// definition attributes for span and metrics
	definition []attribute.KeyValue
	// definitionExtra attributes for span only
	definitionExtra []attribute.KeyValue
	// httpRequest attributes for span and metrics
	httpRequest []attribute.KeyValue
	// httpRequestExtra attributes for span only
	httpRequestExtra []attribute.KeyValue
	// httpResponse attributes for span and metrics
	httpResponse []attribute.KeyValue
	// httpResponseExtra attributes for span only
	httpResponseExtra []attribute.KeyValue
	// httpResponseError attributes for metrics
	httpResponseError []attribute.KeyValue
}

func newAttributes(cfg config, reqDef request.HTTPRequest) *attributes {
	out := &attributes{config: cfg}
	reqURL := reqDef.URL()

	var resultType string
	if v := reflect.TypeOf(reqDef.ResultDef()); v != nil {
		resultType = v.String()
	}

	// Definition base
	out.definition = []attribute.KeyValue{
		attribute.String("definition.method", reqDef.Method()),
		attribute.String("definition.result.type", resultType),
		attribute.String("definition.url.full", mustURLPathUnescape(reqURL.String())),
		attribute.String("definition.url.path", mustURLPathUnescape(reqURL.Path)),
		attribute.String("definition.url.host.full", reqURL.Host),
	}
	if dotPos := strings.IndexByte(reqURL.Host, '.'); dotPos > 0 {
		// Host parts: to trace service name (host prefix) and stack (host suffix).
		out.definition = append(out.definition,
			// Host prefix, e.g. "connection", "encryption", "scheduler" ...
			attribute.String("definition.url.host.prefix", reqURL.Host[:dotPos]),
			// Host suffix, e.g. "keboola.com"
			attribute.String("definition.url.host.suffix", strings.TrimLeft(reqURL.Host[dotPos:], ".")),
		)
	}

	// Definition params
	var headerAttrs []attribute.KeyValue
	for k, v := range reqDef.RequestHeader() {
		value := strings.Join(v, ";")
		if _, found := cfg.redactedHeaders[strings.ToLower(k)]; found {
			value = maskedAttrValue
		}
		headerAttrs = append(headerAttrs, attribute.String("definition.header."+k, value))
	}
	sort.SliceStable(headerAttrs, func(i, j int) bool {
		return headerAttrs[i].Key < headerAttrs[j].Key
	})
	out.definitionExtra = append(out.definitionExtra, headerAttrs...)
	for k, v := range reqDef.QueryParams() {
		out.definitionExtra = append(out.definitionExtra, attribute.String("definition.params.query."+k, cast.ToString(v)))
	}
	for k, v := range reqDef.PathParams() {
		out.definitionExtra = append(out.definitionExtra, attribute.String("definition.params.path."+k, cast.ToString(v)))
	}

	return out
}

func (v *attributes) SetFromRequest(req *http.Request) {
	if req == nil {
		v.httpRequest = nil
		v.httpRequestExtra = nil
		return
	}

	// Base
	v.httpRequest = httpconv.ClientRequest(req)

	// Extra
	var attrs []attribute.KeyValue
	for key, values := range req.Header {
		key = strings.ToLower(key)
		value := strings.Join(values, ";")
		if key == "user-agent" {
			// Skip, it is already present from httpconv
			continue
		}
		if _, found := v.config.redactedHeaders[key]; found {
			value = maskedAttrValue
		}
		attrs = append(attrs, attribute.String("http.header."+key, value))
	}
	sort.SliceStable(attrs, func(i, j int) bool {
		return attrs[i].Key < attrs[j].Key
	})
	v.httpRequestExtra = attrs
}

func (v *attributes) SetFromResponse(res *http.Response, err error) {
	// Success
	if res == nil {
		v.httpResponse = nil
		v.httpResponseExtra = nil
	} else {
		// Base
		v.httpResponse = httpconv.ClientResponse(res)

		// Extra
		var attrs []attribute.KeyValue
		for key, values := range res.Header {
			key = strings.ToLower(key)
			value := strings.Join(values, ";")
			if _, found := v.config.redactedHeaders[key]; found {
				value = maskedAttrValue
			}
			attrs = append(attrs, attribute.String("http.response.header."+key, value))
		}
		sort.SliceStable(attrs, func(i, j int) bool {
			return attrs[i].Key < attrs[j].Key
		})
		v.httpResponseExtra = append(v.httpResponseExtra, attrs...)
	}

	// Error
	var netErr net.Error
	errors.As(err, &netErr)
	v.httpResponseError = []attribute.KeyValue{
		attribute.Bool("http.response.isSuccess", isSuccess(res, err)),
		attribute.Bool("http.response.error.has", err != nil),
		attribute.Bool("http.response.error.net", netErr != nil),
		attribute.Bool("http.response.error.timeout", netErr != nil && netErr.Timeout()),
		attribute.Bool("http.response.error.cancelled", errors.Is(err, context.Canceled)),
		attribute.Bool("http.response.error.deadline_exceeded", errors.Is(err, context.DeadlineExceeded)),
	}
}

func mustURLPathUnescape(in string) string {
	out, err := url.PathUnescape(in)
	if err != nil {
		return in
	}
	return out
}
