package otel

import (
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
	config             config
	definition         []attribute.KeyValue
	definitionParams   []attribute.KeyValue
	httpRequest        []attribute.KeyValue
	httpRequestParams  []attribute.KeyValue
	httpResponse       []attribute.KeyValue
	httpResponseParams []attribute.KeyValue
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
	out.definitionParams = append(out.definitionParams, headerAttrs...)
	for k, v := range reqDef.QueryParams() {
		out.definitionParams = append(out.definitionParams, attribute.String("definition.params.query."+k, cast.ToString(v)))
	}
	for k, v := range reqDef.PathParams() {
		out.definitionParams = append(out.definitionParams, attribute.String("definition.params.path."+k, cast.ToString(v)))
	}

	return out
}

func (v *attributes) SetFromRequest(req *http.Request) {
	if req == nil {
		v.httpRequest = nil
		v.httpRequestParams = nil
	} else {
		v.httpRequest = httpconv.ClientRequest(req)
		v.httpRequestParams = nil
		var headerAttrs []attribute.KeyValue
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
			headerAttrs = append(headerAttrs, attribute.String("http.header."+key, value))
		}
		sort.SliceStable(headerAttrs, func(i, j int) bool {
			return headerAttrs[i].Key < headerAttrs[j].Key
		})
		v.httpRequestParams = append(v.httpRequestParams, headerAttrs...)
	}
}

func (v *attributes) SetFromResponse(res *http.Response) {
	if res == nil {
		v.httpResponse = nil
		v.httpResponseParams = nil
	} else {
		v.httpResponse = httpconv.ClientResponse(res)
		v.httpResponseParams = nil
		var headerAttrs []attribute.KeyValue
		for key, values := range res.Header {
			key = strings.ToLower(key)
			value := strings.Join(values, ";")
			if _, found := v.config.redactedHeaders[key]; found {
				value = maskedAttrValue
			}
			headerAttrs = append(headerAttrs, attribute.String("http.response.header."+key, value))
		}
		sort.SliceStable(headerAttrs, func(i, j int) bool {
			return headerAttrs[i].Key < headerAttrs[j].Key
		})
		v.httpResponseParams = append(v.httpResponseParams, headerAttrs...)
	}
}

func mustURLPathUnescape(in string) string {
	out, err := url.PathUnescape(in)
	if err != nil {
		return in
	}
	return out
}
