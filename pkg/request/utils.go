package request

import (
	jsonlib "encoding/json"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
)

// ToFormBody converts a JSON like map to form body map, any type is mapped to string.
func ToFormBody(in map[string]any) (out map[string]string) {
	out = make(map[string]string)
	for k, v := range in {
		ty := reflect.TypeOf(v)
		if ty.Kind() == reflect.Slice {
			for i, s := range v.([]string) {
				out[fmt.Sprintf("%s[%d]", k, i)] = s
			}
		} else if ty.Kind() == reflect.Map && ty.Elem().Kind() == reflect.String {
			for i, s := range v.(map[string]string) {
				out[fmt.Sprintf("%s[%s]", k, i)] = s
			}
		} else {
			out[k] = castToString(v)
		}
	}
	return out
}

// StructToMap converts a struct to values map.
// Only defined allowedFields are converted.
// If allowedFields = nil, then all fields are exported.
//
// Field name is read from `writeas` tag or from "json" tag as fallback.
// Field with tag `readonly:"true"` is ignored.
// Field with tag `writeoptional` is exported only if value is not empty.
func StructToMap(in any, allowedFields []string) (out map[string]any) {
	out = make(map[string]any)
	structToMap(reflect.ValueOf(in), out, allowedFields)
	return out
}

func structToMap(in reflect.Value, out map[string]any, allowedFields []string) {
	// Initialize
	for in.Kind() == reflect.Ptr || in.Kind() == reflect.Interface {
		in = in.Elem()
	}
	t := in.Type()

	// Convert allowed slice to map
	allowed := make(map[string]bool)
	for _, field := range allowedFields {
		allowed[field] = true
	}

	// Iterate over fields
	numFields := t.NumField()
	for i := range numFields {
		field := t.Field(i)
		fieldValue := in.Field(i)

		// Process embedded type
		if field.Anonymous {
			structToMap(fieldValue, out, allowedFields)
			continue
		}

		// Skip filed with tag `readonly:"true"`
		if field.Tag.Get("readonly") == "true" {
			continue
		}

		// Skip field with tag `writeoptional:"true"` and empty value
		if field.Tag.Get("writeoptional") == "true" && fieldValue.IsZero() {
			continue
		}

		// Get field name
		var fieldName string
		if v := field.Tag.Get("writeas"); v != "" {
			fieldName = v
		} else if v := strings.Split(field.Tag.Get("json"), ",")[0]; v != "" {
			fieldName = v
		} else {
			panic(fmt.Errorf(`field "%s" of %s has no json name`, field.Name, t.String()))
		}

		// Skip ignored fields
		if fieldName == "-" {
			continue
		}

		// Is allowed?
		if len(allowedFields) > 0 && !allowed[fieldName] {
			continue
		}

		// Ok, add to map
		out[fieldName] = fieldValue.Interface()
	}
}

func cloneParams(in map[string]string) (out map[string]string) {
	out = make(map[string]string)
	maps.Copy(out, in)
	return out
}

func cloneURLValues(in url.Values) (out url.Values) {
	out = make(url.Values)
	for k, values := range in {
		for _, v := range values {
			out.Add(k, v)
		}
	}
	return out
}

func castToString(v any) string {
	// Ordered map
	if orderedMap, ok := v.(*orderedmap.OrderedMap); ok {
		// Standard json encoding library is used.
		// JsonIter lib returns non-compact JSON,
		// if custom OrderedMap.MarshalJSON method is used.
		if v, err := jsonlib.Marshal(orderedMap); err != nil {
			panic(fmt.Errorf(`cannot cast %T to string %w`, v, err))
		} else {
			return string(v)
		}
	}

	// Other types
	if v, err := cast.ToStringE(v); err != nil {
		panic(fmt.Errorf(`cannot cast %T to string %w`, v, err))
	} else {
		return v
	}
}
