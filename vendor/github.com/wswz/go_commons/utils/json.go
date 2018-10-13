package utils

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/jmoiron/jsonq"
)

var (
	UpperToUnderRegex = regexp.MustCompile("[A-Z]")
)

func FilterByKeys(result interface{}, keys ...string) interface{} {
	if len(keys) < 1 {
		return result
	}
	v := reflect.ValueOf(result)
	t := reflect.TypeOf(result)
	switch t.Kind() {
	case reflect.Array, reflect.Slice:
		rs := []interface{}{}
		for i := 0; i < v.Len(); i++ {
			rs = append(rs, FilterEntityByKeys(v.Index(i).Interface(), keys...))
		}
		return rs
	default:
		return FilterByKeys(result, keys...)
	}
}

func FilterEntityByKeys(result interface{}, keys ...string) interface{} {
	if len(keys) < 1 {
		return result
	}
	res := map[string]interface{}{}
	query := jsonq.NewQuery(result)
	for _, key := range keys {
		res[key], _ = query.Object(key)
	}
	return res
}

func MergeMaps(results ...interface{}) map[string]interface{} {
	res := map[string]interface{}{}
	for _, r := range results {
		mp, _ := ToMap(r, "json")
		for k, v := range mp {
			res[k] = v
		}
	}
	return res
}

func ToMap(in interface{}, tag string) (map[string]interface{}, error) {
	out := make(map[string]interface{})
	v := reflect.ValueOf(in)
	t := reflect.TypeOf(in)

	switch t.Kind() {
	case reflect.Ptr:
		return ToMap(v.Elem().Interface(), tag)

	case reflect.Struct:
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			// gets us a StructField
			fi := typ.Field(i)
			tagv := fi.Tag.Get(tag)
			if tagv == "-" {
				continue
			}
			if tagv == "" {
				tagv = fi.Name
			}
			if v.Field(i).CanInterface() {
				out[tagv] = v.Field(i).Interface()
			}
		}
		return out, nil

	case reflect.Map:
		for _, key := range v.MapKeys() {
			if v.MapIndex(key).CanInterface() {
				out[key.String()] = v.MapIndex(key).Interface()
			}
		}
		return out, nil

	default:
		return nil, fmt.Errorf("ToMap only accepts structs ptr maps; got %T", v)
	}
}

func ToLowerJson(in interface{}, tag string) (res interface{}, err error) {
	if in == nil {
		return
	}
	out := make(map[string]interface{})
	v := reflect.ValueOf(in)
	t := reflect.TypeOf(in)

	//TODO why
	if v == reflect.Zero(t) {
		return
	}

	switch t.Kind() {
	case reflect.Ptr:
		return ToLowerJson(v.Elem().Interface(), tag)

	case reflect.Struct:
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			// gets us a StructField
			fi := typ.Field(i)
			tagv := fi.Tag.Get(tag)
			if tagv == "-" {
				continue
			}
			if tagv == "" {
				tagv = GetUnderKey(fi.Name)
			}
			if v.Field(i).CanInterface() {
				ir, err := ToLowerJson(v.Field(i).Interface(), tag)
				if err != nil {
					return nil, err
				}
				out[tagv] = ir
			}
		}
		return out, nil

	case reflect.Map:
		for _, key := range v.MapKeys() {
			if v.MapIndex(key).CanInterface() {
				ir, err := ToLowerJson(v.MapIndex(key).Interface(), tag)
				if err != nil {
					return nil, err
				}
				out[GetUnderKey(key.String())] = ir
			}
		}
		return out, nil

	case reflect.Array, reflect.Slice:
		rs := []interface{}{}
		for i := 0; i < v.Len(); i++ {
			if !v.Index(i).CanInterface() {
				continue
			}
			ir, err := ToLowerJson(v.Index(i).Interface(), tag)
			if err != nil {
				return nil, err
			}
			rs = append(rs, ir)
		}
		return rs, nil

	default:
		if v.CanInterface() {
			return v.Interface(), nil
		}
		return nil, nil
	}
}

func GetUnderKey(key string) string {
	key = UpperToUnderRegex.ReplaceAllStringFunc(key, func(k string) string {
		return "_" + strings.ToLower(k)
	})
	key = strings.Trim(key, "_")
	return key
}
