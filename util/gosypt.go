package util

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

const (
	GosyptPrefixDefault = "ENC("
	GosyptSuffxiDefault = ")"
	GosyptAlgorithm     = "AESWITHHEXANDBASE64"
)

/* Golang Simple Encrypt, simulate jasypt */
type Gosypt struct {
	prefix    string
	suffix    string
	algorithm string
}

var Gsypt = &Gosypt{
	prefix:    GosyptPrefixDefault,
	suffix:    GosyptSuffxiDefault,
	algorithm: GosyptAlgorithm,
}

func (gsypt *Gosypt) ensurePassword(password string) string {
	if !strings.HasPrefix(password, gsypt.prefix) || !strings.HasSuffix(password, gsypt.suffix) {
		return password
	}
	passwd := strings.TrimSuffix(strings.TrimPrefix(password, gsypt.prefix), gsypt.suffix)
	if gsypt.algorithm == GosyptAlgorithm {
		return AesDecryptECB(passwd)
	}
	return password
}

func (gsypt *Gosypt) SetAttribution(prefix, suffix, algorithm string) {
	gsypt.prefix = prefix
	gsypt.suffix = suffix
	gsypt.algorithm = algorithm
}

func (gsypt *Gosypt) Unmarshal(v interface{}) error {
	rt := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)

	if rt.Kind() != reflect.Ptr {
		return errors.Wrap(nil, "invalid args, expect ptr")
	}

	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}

	if rt.Kind() == reflect.Struct {
		v, err := gsypt.structHandle(rt, rv)
		if err != nil {
			return err
		}
		rv.Set(v)
	} else if rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
		v, err := gsypt.sliceHandle(rt, rv)
		if err != nil {
			return err
		}
		rv.Set(v)
	} else if rt.Kind() == reflect.Map {
		v, err := gsypt.mapHandle(rt, rv)
		if err != nil {
			return err
		}
		rv.Set(v)
	} else if rt.Kind() == reflect.Interface {
		v, err := gsypt.interfaceHandle(rt, rv)
		if err != nil {
			return err
		}
		rv.Set(v)
	} else if rt.Kind() == reflect.String {
		rv.Set(gsypt.stringHandle(rv))
	}

	return nil
}

func (gsypt *Gosypt) sliceHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	for j := 0; j < rv.Len(); j++ {
		if rt.Elem().Kind() == reflect.String {
			rv.Index(j).Set(gsypt.stringHandle(rv.Index(j)))
		} else {
			if err := gsypt.Unmarshal(rv.Index(j).Addr().Interface()); err != nil {
				return rv, err
			}
		}
	}
	return rv, nil
}

func (gsypt *Gosypt) mapHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	for _, k := range rv.MapKeys() {
		key := k.Convert(rv.Type().Key())
		if rt.Elem().Kind() == reflect.String {
			v := gsypt.ensurePassword(rv.MapIndex(key).String())
			rv.SetMapIndex(key, reflect.ValueOf(v))
		} else {
			v := rv.MapIndex(key).Interface()
			if err := gsypt.Unmarshal(&v); err != nil {
				return rv, err
			}
			rv.SetMapIndex(key, reflect.ValueOf(v))
		}
	}
	return rv, nil
}

func (gsypt *Gosypt) interfaceHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	//todo
	return rv, nil
}

func (gsypt *Gosypt) structHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	for i := 0; i < rt.NumField(); i++ {
		rtf := rt.Field(i)
		rvf := rv.Field(i)

		rtt := rtf.Type
		for rtt.Kind() == reflect.Ptr {
			rtt = rtt.Elem()
		}

		if rtt.Kind() == reflect.String {
			rv.Field(i).Set(gsypt.stringHandle(rvf))
		} else {
			if err := gsypt.Unmarshal(rvf.Addr().Interface()); err != nil {
				return rv, err
			}
		}
	}
	return rv, nil
}

func (gsypt *Gosypt) stringHandle(rv reflect.Value) reflect.Value {
	rv.SetString(gsypt.ensurePassword(rv.String()))
	return rv
}
