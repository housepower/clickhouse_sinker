package util

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

const (
	GosyptPrefixDefault = "ENC("
	GosyptSuffxiDefault = ")"
	GosyptAlgorithm     = "AESWITHHEXANDBASE64"
	GosyptSM4Algorithm  = "SM4"
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

func (gsypt *Gosypt) ensurePassword(password string) (string, error) {
	if !strings.HasPrefix(password, gsypt.prefix) || !strings.HasSuffix(password, gsypt.suffix) {
		return password, nil
	}
	passwd := strings.TrimSuffix(strings.TrimPrefix(password, gsypt.prefix), gsypt.suffix)
	if gsypt.algorithm == GosyptAlgorithm {
		return AesDecryptECB(passwd), nil
	}
	if gsypt.algorithm == GosyptSM4Algorithm {
		return SM4Decrypt(passwd)
	}
	return "", fmt.Errorf("unsupported encryption algorithm %q", gsypt.algorithm)
}

// SetAlgorithm switches the algorithm used for every ENC(...) value, both in
// command options and in config content. Empty means the default AES. SM4
// requires the key in the SINKER_SM4_KEY environment variable.
func (gsypt *Gosypt) SetAlgorithm(algorithm string) error {
	if algorithm == "" {
		algorithm = GosyptAlgorithm
	}
	if algorithm != GosyptAlgorithm && algorithm != GosyptSM4Algorithm {
		return fmt.Errorf("unsupported encryption algorithm %q", algorithm)
	}
	if algorithm == GosyptSM4Algorithm {
		if _, err := sm4Key(); err != nil {
			return err
		}
	}
	gsypt.algorithm = algorithm
	return nil
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
		v, err := gsypt.stringHandle(rv)
		if err != nil {
			return err
		}
		rv.Set(v)
	}

	return nil
}

func (gsypt *Gosypt) sliceHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	for j := 0; j < rv.Len(); j++ {
		if rt.Elem().Kind() == reflect.String {
			v, err := gsypt.stringHandle(rv.Index(j))
			if err != nil {
				return rv, err
			}
			rv.Index(j).Set(v)
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
			v, err := gsypt.ensurePassword(rv.MapIndex(key).String())
			if err != nil {
				return rv, err
			}
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
	if rv.IsNil() {
		return rv, nil
	}
	v := rv.Elem()
	dup := reflect.New(v.Type())
	dup.Elem().Set(v)
	if err := gsypt.Unmarshal(dup.Interface()); err != nil {
		return rv, err
	}
	rv.Set(dup.Elem())
	return rv, nil
}

func (gsypt *Gosypt) structHandle(rt reflect.Type, rv reflect.Value) (reflect.Value, error) {
	for i := 0; i < rt.NumField(); i++ {
		rtf := rt.Field(i)
		rvf := rv.Field(i)

		// Unexported fields can neither be inspected nor set through reflection.
		// Fields tagged `gsypt:"-"` are pass-through by contract (e.g. header
		// values) and must not be decrypted even when they hold ENC(...).
		if rtf.PkgPath != "" || rtf.Tag.Get("gsypt") == "-" {
			continue
		}

		rtt := rtf.Type
		for rtt.Kind() == reflect.Ptr {
			rtt = rtt.Elem()
		}

		if rtt.Kind() == reflect.String {
			v, err := gsypt.stringHandle(rvf)
			if err != nil {
				return rv, err
			}
			rv.Field(i).Set(v)
		} else {
			if err := gsypt.Unmarshal(rvf.Addr().Interface()); err != nil {
				return rv, err
			}
		}
	}
	return rv, nil
}

func (gsypt *Gosypt) stringHandle(rv reflect.Value) (reflect.Value, error) {
	v, err := gsypt.ensurePassword(rv.String())
	if err != nil {
		return rv, err
	}
	rv.SetString(v)
	return rv, nil
}
