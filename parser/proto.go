package parser

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/exp/constraints"
	"io"
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate mockgen -source=../vendor/github.com/confluentinc/confluent-kafka-go/schemaregistry/schemaregistry_client.go -imports=schemaregistry=github.com/confluentinc/confluent-kafka-go/schemaregistry -package=parser -mock_names=Client=MockSchemaRegistryClient -destination=schema_registry_mock_test.go

// ProtoParser knows how to get data from proto format.
type ProtoParser struct {
	pp           *Pool
	deserializer *ProtoDeserializer
}

// Parse parses bytes into metric that knows how.
func (p *ProtoParser) Parse(bs []byte) (model.Metric, error) {
	msg, err := p.deserializer.ToDynamicMessage(bs)
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}
	return &ProtoMetric{pp: p.pp, msg: msg}, nil
}

type ProtoMetric struct {
	pp  *Pool
	msg *dynamic.Message
}

func (m *ProtoMetric) GetBool(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	if value == nil {
		return getDefaultBool(nullable)
	}

	return getBool(reflect.ValueOf(value), nullable)
}

func (m *ProtoMetric) GetInt8(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getIntFromProto[int8](value, nullable, math.MinInt8, math.MaxInt8)
}

func (m *ProtoMetric) GetInt16(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getIntFromProto[int16](value, nullable, math.MinInt16, math.MaxInt16)
}

func (m *ProtoMetric) GetInt32(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getIntFromProto[int32](value, nullable, math.MinInt32, math.MaxInt32)
}

func (m *ProtoMetric) GetInt64(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getIntFromProto[int64](value, nullable, math.MinInt64, math.MaxInt64)
}

func (m *ProtoMetric) GetUint8(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getUIntFromProto[uint8](value, nullable, math.MaxUint8)
}

func (m *ProtoMetric) GetUint16(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getUIntFromProto[uint16](value, nullable, math.MaxUint16)
}

func (m *ProtoMetric) GetUint32(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getUIntFromProto[uint32](value, nullable, math.MaxUint32)
}

func (m *ProtoMetric) GetUint64(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getUIntFromProto[uint64](value, nullable, math.MaxUint64)
}

func (m *ProtoMetric) GetFloat32(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getFloatFromProto[float32](value, nullable, math.MaxFloat32)
}

func (m *ProtoMetric) GetFloat64(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	return getFloatFromProto[float64](value, nullable, math.MaxFloat64)
}

func (m *ProtoMetric) GetDecimal(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	if value == nil {
		return getDefaultDecimal(nullable)
	}

	rv := reflect.ValueOf(value)
	return getDecimal(rv, nullable)
}

func (m *ProtoMetric) GetDateTime(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	if value == nil {
		return getDefaultDateTime(nullable)
	}

	rv := reflect.ValueOf(value)
	if ts, ok := rv.Interface().(*timestamppb.Timestamp); ok {
		return ts.AsTime()
	}
	if rv.Kind() == reflect.String {
		val, err := m.pp.ParseDateTime(key, value.(string))
		if err != nil {
			return getDefaultDateTime(nullable)
		}
		return val
	}

	return getDefaultDateTime(nullable)
}

func (m *ProtoMetric) GetString(key string, nullable bool) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	if value == nil {
		if nullable {
			return nil
		}
		return ""
	}

	return getString(reflect.ValueOf(value), nullable)
}

func (m *ProtoMetric) GetArray(key string, t int) interface{} {
	value, _ := m.msg.TryGetFieldByName(key)
	if value == nil {
		return nil
	}

	switch t {
	case model.Bool:
		arr := make([]bool, 0)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			item := getBool(reflect.ValueOf(rv.Index(i).Interface()), false).(bool)
			arr = append(arr, item)
		}
		return arr
	case model.Int8:
		return getIntSliceFromProto[int8](value, math.MinInt8, math.MaxInt8)
	case model.Int16:
		return getIntSliceFromProto[int16](value, math.MinInt16, math.MaxInt16)
	case model.Int32:
		return getIntSliceFromProto[int32](value, math.MinInt32, math.MaxInt32)
	case model.Int64:
		return getIntSliceFromProto[int64](value, math.MinInt64, math.MaxInt64)
	case model.UInt8:
		return getUIntSliceFromProto[uint8](value, math.MaxUint8)
	case model.UInt16:
		return getUIntSliceFromProto[uint16](value, math.MaxUint16)
	case model.UInt32:
		return getUIntSliceFromProto[uint32](value, math.MaxUint32)
	case model.UInt64:
		return getUIntSliceFromProto[uint64](value, math.MaxUint64)
	case model.Float32:
		return getFloatSliceFromProto[float32](value, math.MaxFloat32)
	case model.Float64:
		return getFloatSliceFromProto[float64](value, math.MaxFloat64)
	case model.Decimal:
		arr := make([]decimal.Decimal, 0)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			item := getDecimal(reflect.ValueOf(rv.Index(i).Interface()), false).(decimal.Decimal)
			arr = append(arr, item)
		}
		return arr
	case model.String:
		arr := make([]string, 0)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			item := getString(reflect.ValueOf(rv.Index(i).Interface()), false).(string)
			arr = append(arr, item)
		}
		return arr
	case model.DateTime:
		arr := make([]time.Time, 0)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			item := getDateTime(reflect.ValueOf(rv.Index(i).Interface()), false).(time.Time)
			arr = append(arr, item)
		}
		return arr
	default:
		util.Logger.Fatal(fmt.Sprintf("unsupported array type %v", t))
	}

	return nil
}

func (m *ProtoMetric) GetNewKeys(knownKeys, newKeys, warnKeys *sync.Map, white, black *regexp.Regexp, partition int, offset int64) bool {
	// dynamic schema not supported
	return false
}

func getIntSliceFromProto[T constraints.Signed](fieldVal interface{}, min, max int64) []T {
	arr := make([]T, 0)
	rv := reflect.ValueOf(fieldVal)
	for i := 0; i < rv.Len(); i++ {
		item := getIntFromProto[T](rv.Index(i).Interface(), false, min, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getIntFromProto[T constraints.Signed](fieldVal interface{}, nullable bool, min, max int64) interface{} {
	if fieldVal == nil {
		return getDefaultInt[T](nullable)
	}

	rv := reflect.ValueOf(fieldVal)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		int64Val := rv.Int()
		if int64Val < min {
			return T(min)
		}
		if int64Val > max {
			return T(max)
		}
		return T(int64Val)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		uint64Val := rv.Uint()
		if uint64Val > uint64(max) {
			return T(max)
		}
		return T(uint64Val)
	case reflect.Bool:
		if rv.Bool() {
			return T(1)
		}
		return T(0)
	default:
		return getDefaultInt[T](nullable)
	}
}

func getUIntSliceFromProto[T constraints.Unsigned](fieldVal interface{}, max uint64) []T {
	arr := make([]T, 0)
	rv := reflect.ValueOf(fieldVal)
	for i := 0; i < rv.Len(); i++ {
		item := getUIntFromProto[T](rv.Index(i).Interface(), false, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getUIntFromProto[T constraints.Unsigned](fieldVal interface{}, nullable bool, max uint64) interface{} {
	if fieldVal == nil {
		return getDefaultInt[T](nullable)
	}

	rv := reflect.ValueOf(fieldVal)
	switch rv.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		uint64Val := rv.Uint()
		if uint64Val > max {
			return T(max)
		}
		return T(uint64Val)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intV := rv.Int()
		if intV < 0 {
			return getDefaultInt[T](nullable)
		}
		if uint64(intV) > max {
			return T(max)
		}
		return T(intV)
	case reflect.Bool:
		if rv.Bool() {
			return T(1)
		}
		return T(0)
	default:
		return getDefaultInt[T](nullable)
	}
}

func getFloatSliceFromProto[T constraints.Float](fieldVal interface{}, max float64) interface{} {
	arr := make([]T, 0)
	rv := reflect.ValueOf(fieldVal)
	for i := 0; i < rv.Len(); i++ {
		item := getFloatFromProto[T](rv.Index(i).Interface(), false, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getFloatFromProto[T constraints.Float](fieldVal interface{}, nullable bool, max float64) interface{} {
	if fieldVal == nil {
		return getDefaultFloat[T](nullable)
	}

	rv := reflect.ValueOf(fieldVal)
	if rv.CanFloat() {
		float64Val := rv.Float()
		if float64Val > max {
			return T(max)
		}
		return T(float64Val)
	}
	return getDefaultFloat[T](nullable)
}

func getBool(value reflect.Value, nullable bool) interface{} {
	if value.Kind() == reflect.Bool {
		return value.Bool()
	}
	return getDefaultBool(nullable)
}

func getDecimal(value reflect.Value, nullable bool) interface{} {
	if value.CanFloat() {
		return decimal.NewFromFloat(value.Float())
	}
	return getDefaultDecimal(nullable)
}

func getDateTime(value reflect.Value, nullable bool) interface{} {
	if ts, ok := value.Interface().(*timestamppb.Timestamp); ok {
		return ts.AsTime()
	}
	return getDefaultDateTime(nullable)
}

func getString(value reflect.Value, nullable bool) interface{} {
	if value.Kind() == reflect.String {
		return value.String()
	}
	return fmt.Sprintf("%v", value.Interface())
}

// ProtoDeserializer represents a Protobuf deserializer.
type ProtoDeserializer struct {
	schemaRegistry   schemaregistry.Client
	baseDeserializer *serde.BaseDeserializer
	topic            string
}

func (p *ProtoDeserializer) ToDynamicMessage(bytes []byte) (*dynamic.Message, error) {
	if len(bytes) == 0 {
		return nil, nil
	}

	schemaInfo, err := p.baseDeserializer.GetSchema(p.topic, bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema info: %w", err)
	}
	fileDesc, err := p.toFileDescriptor(schemaInfo)
	if err != nil {
		return nil, err
	}
	bytesRead, msgIndexes, err := readMessageIndexes(bytes[5:])
	if err != nil {
		return nil, err
	}
	messageDesc, err := toMessageDescriptor(fileDesc, msgIndexes)
	if err != nil {
		return nil, err
	}

	msg := dynamic.NewMessage(messageDesc)
	if err := msg.Unmarshal(bytes[5+bytesRead:]); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value into protobuf message: %w", err)
	}

	return msg, nil
}

func (p *ProtoDeserializer) toFileDescriptor(info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(p.schemaRegistry, info, deps)
	if err != nil {
		return nil, err
	}
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema string
			if filename == "." {
				schema = info.Schema
			} else {
				schema = deps[filename]
			}
			if schema == "" {
				// these may be google types
				return nil, errors.Newf("unknown reference")
			}
			return io.NopCloser(strings.NewReader(schema)), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(".")
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("could not resolve schema")
	}
	return fileDescriptors[0], nil
}

func readMessageIndexes(bytes []byte) (int, []int, error) {
	arrayLen, bytesRead := binary.Varint(bytes)
	if bytesRead <= 0 {
		return bytesRead, nil, fmt.Errorf("unable to read message indexes")
	}
	if arrayLen == 0 {
		// handle the optimization for the first message in the schema
		return bytesRead, []int{0}, nil
	}
	msgIndexes := make([]int, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		idx, read := binary.Varint(bytes[bytesRead:])
		if read <= 0 {
			return bytesRead, nil, fmt.Errorf("unable to read message indexes")
		}
		bytesRead += read
		msgIndexes[i] = int(idx)
	}
	return bytesRead, msgIndexes, nil
}

func toMessageDescriptor(descriptor desc.Descriptor, msgIndexes []int) (*desc.MessageDescriptor, error) {
	index := msgIndexes[0]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetMessageTypes()[index], nil
		}
		return toMessageDescriptor(v.GetMessageTypes()[index], msgIndexes[1:])
	case *desc.MessageDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetNestedMessageTypes()[index], nil
		}
		return toMessageDescriptor(v.GetNestedMessageTypes()[index], msgIndexes[1:])
	default:
		return nil, fmt.Errorf("unexpected type")
	}
}
