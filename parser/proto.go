package parser

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"math"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
)

//go:generate mockgen -source=../vendor/github.com/confluentinc/confluent-kafka-go/schemaregistry/schemaregistry_client.go -imports=schemaregistry=github.com/confluentinc/confluent-kafka-go/schemaregistry -package=parser -mock_names=Client=MockSchemaRegistryClient -destination=schema_registry_mock_test.go

const magicByte byte = 0x0

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
	msg protoreflect.Message
}

func (m *ProtoMetric) GetBool(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultBool(nullable)
	}

	return getProtoBool(field.Kind(), m.msg.Get(field), nullable)
}

func (m *ProtoMetric) GetInt8(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[int8](nullable)
	}

	return getIntFromProto[int8](field.Kind(), m.msg.Get(field), nullable, math.MinInt8, math.MaxInt8)
}

func (m *ProtoMetric) GetInt16(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[int16](nullable)
	}

	return getIntFromProto[int16](field.Kind(), m.msg.Get(field), nullable, math.MinInt16, math.MaxInt16)
}

func (m *ProtoMetric) GetInt32(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[int32](nullable)
	}

	return getIntFromProto[int32](field.Kind(), m.msg.Get(field), nullable, math.MinInt32, math.MaxInt32)

}

func (m *ProtoMetric) GetInt64(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[int64](nullable)
	}

	return getIntFromProto[int64](field.Kind(), m.msg.Get(field), nullable, math.MinInt64, math.MaxInt64)
}

func (m *ProtoMetric) GetUint8(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[uint8](nullable)
	}

	return getUIntFromProto[uint8](field.Kind(), m.msg.Get(field), nullable, math.MaxUint8)
}

func (m *ProtoMetric) GetUint16(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[uint16](nullable)
	}

	return getUIntFromProto[uint16](field.Kind(), m.msg.Get(field), nullable, math.MaxUint16)
}

func (m *ProtoMetric) GetUint32(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[uint32](nullable)
	}

	return getUIntFromProto[uint32](field.Kind(), m.msg.Get(field), nullable, math.MaxUint32)
}

func (m *ProtoMetric) GetUint64(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultInt[uint64](nullable)
	}

	return getUIntFromProto[uint64](field.Kind(), m.msg.Get(field), nullable, math.MaxUint64)
}

func (m *ProtoMetric) GetFloat32(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultFloat[float32](nullable)
	}

	return getFloatFromProto[float32](field.Kind(), m.msg.Get(field), nullable, math.MaxFloat32)
}

func (m *ProtoMetric) GetFloat64(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultFloat[float64](nullable)
	}

	return getFloatFromProto[float64](field.Kind(), m.msg.Get(field), nullable, math.MaxFloat64)
}

func (m *ProtoMetric) GetDecimal(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultDecimal(nullable)
	}

	return getProtoDecimal(field.Kind(), m.msg.Get(field), nullable)
}

func (m *ProtoMetric) GetDateTime(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil {
		return getDefaultDateTime(nullable)
	}

	if field.Kind() == protoreflect.MessageKind {
		return getProtoDateTime(m.msg.Get(field), nullable)
	}
	if field.Kind() != protoreflect.StringKind {
		return getDefaultDateTime(nullable)
	}

	val, err := m.pp.ParseDateTime(key, m.msg.Get(field).String())
	if err != nil {
		return getDefaultDateTime(nullable)
	}

	return val
}

func (m *ProtoMetric) GetString(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil {
		return getDefaultString(nullable, "")
	}

	if !field.IsList() {
		return getProtoString(m.msg.Get(field))
	}

	list := m.msg.Get(field).List()
	strList := make([]string, list.Len())
	for i := 0; i < list.Len(); i++ {
		strList[i] = getProtoString(list.Get(i))
	}
	return "[" + strings.Join(strList, " ") + "]"
}

func (m *ProtoMetric) GetUUID(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultString(nullable, zeroUUID)
	}

	if v := getProtoString(m.msg.Get(field)); v != "" {
		return v
	}
	return zeroUUID
}

func (m *ProtoMetric) GetObject(key string, nullable bool) interface{} {
	return nil
}

func (m *ProtoMetric) GetMap(key string, typeInfo *model.TypeInfo) interface{} {
	regularMap := model.NewOrderedMap()
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil {
		return regularMap
	}

	if !field.IsMap() {
		return regularMap
	}
	a := field.Kind()
	_ = a
	data := m.msg.Get(field).Map()
	data.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		switch typeInfo.MapValue.Type {
		case model.Map:
			regularMap.Put(key.Interface(), protoObjectToMap(value))
		case model.DateTime:
			regularMap.Put(key.Interface(), getProtoDateTime(value, typeInfo.MapValue.Nullable))
		default:
			regularMap.Put(key.Interface(), value.Interface())
		}

		return true
	})

	return regularMap
}

func protoObjectToMap(value protoreflect.Value) (resultMap *model.OrderedMap) {
	resultMap = model.NewOrderedMap()

	_, ok := value.Interface().(protoreflect.Message)
	if !ok {
		return resultMap
	}

	valMap := value.Message()
	valMap.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if descriptor.IsList() {
			resultMap.Put(descriptor.JSONName(), getProtoList(value.List()))
			return true
		}
		if descriptor.IsMap() {
			resultMap.Put(descriptor.JSONName(), protoObjectToMap(value))
			return true
		}

		resultMap.Put(descriptor.JSONName(), value.Interface())
		return true
	})

	return resultMap
}

func getProtoList(list protoreflect.List) []any {
	var res []any
	for i := range list.Len() {
		res = append(res, list.Get(i).Interface())
	}

	return res
}

func (m *ProtoMetric) GetIPv4(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultString(nullable, net.IPv4zero.String())
	}

	if v := getProtoString(m.msg.Get(field)); v != "" {
		return v
	}
	return net.IPv4zero.String()
}

func (m *ProtoMetric) GetIPv6(key string, nullable bool) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil || field.IsList() {
		return getDefaultString(nullable, net.IPv6zero.String())
	}

	if v := getProtoString(m.msg.Get(field)); v != "" {
		return v
	}
	return net.IPv6zero.String()
}

func (m *ProtoMetric) GetArray(key string, t int) interface{} {
	field := m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))
	if field == nil {
		return nil
	}
	if !field.IsList() {
		return nil
	}

	list := m.msg.Get(m.msg.Descriptor().Fields().ByName(protoreflect.Name(key))).List()
	if list == nil {
		return nil
	}

	switch t {
	case model.Bool:
		arr := make([]bool, 0)
		for i := 0; i < list.Len(); i++ {
			item := getProtoBool(field.Kind(), list.Get(i), false).(bool)
			arr = append(arr, item)
		}
		return arr
	case model.Int8:
		return getIntSliceFromProto[int8](field.Kind(), list, math.MinInt8, math.MaxInt8)
	case model.Int16:
		return getIntSliceFromProto[int16](field.Kind(), list, math.MinInt16, math.MaxInt16)
	case model.Int32:
		return getIntSliceFromProto[int32](field.Kind(), list, math.MinInt32, math.MaxInt32)
	case model.Int64:
		return getIntSliceFromProto[int64](field.Kind(), list, math.MinInt64, math.MaxInt64)
	case model.UInt8:
		return getUIntSliceFromProto[uint8](field.Kind(), list, math.MaxUint8)
	case model.UInt16:
		return getUIntSliceFromProto[uint16](field.Kind(), list, math.MaxUint16)
	case model.UInt32:
		return getUIntSliceFromProto[uint32](field.Kind(), list, math.MaxUint32)
	case model.UInt64:
		return getUIntSliceFromProto[uint64](field.Kind(), list, math.MaxUint64)
	case model.Float32:
		return getFloatSliceFromProto[float32](field.Kind(), list, math.MaxFloat32)
	case model.Float64:
		return getFloatSliceFromProto[float64](field.Kind(), list, math.MaxFloat64)
	case model.Decimal:
		arr := make([]decimal.Decimal, 0)
		for i := 0; i < list.Len(); i++ {
			item := getProtoDecimal(field.Kind(), list.Get(i), false).(decimal.Decimal)
			arr = append(arr, item)
		}
		return arr
	case model.String:
		arr := make([]string, 0)
		for i := 0; i < list.Len(); i++ {
			item := getProtoString(list.Get(i))
			arr = append(arr, item)
		}
		return arr
	case model.UUID:
		arr := make([]string, 0)
		for i := 0; i < list.Len(); i++ {
			item := getProtoString(list.Get(i))
			if item == "" {
				item = zeroUUID
			}
			arr = append(arr, item)
		}
		return arr
	case model.DateTime:
		arr := make([]time.Time, 0)
		for i := 0; i < list.Len(); i++ {
			item := getProtoDateTime(list.Get(i), false).(time.Time)
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

func getIntSliceFromProto[T constraints.Signed](
	kind protoreflect.Kind,
	list protoreflect.List,
	min, max int64,
) []T {
	arr := make([]T, 0)
	for i := 0; i < list.Len(); i++ {
		item := getIntFromProto[T](kind, list.Get(i), false, min, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getIntFromProto[T constraints.Signed](
	kind protoreflect.Kind,
	value protoreflect.Value,
	nullable bool,
	min,
	max int64,
) interface{} {
	if !value.IsValid() {
		return getDefaultInt[T](nullable)
	}

	switch kind {
	case protoreflect.Int64Kind, protoreflect.Int32Kind,
		protoreflect.Sint64Kind, protoreflect.Sint32Kind,
		protoreflect.Sfixed64Kind, protoreflect.Sfixed32Kind:

		int64Val := value.Int()
		if int64Val < min {
			return T(min)
		}
		if int64Val > max {
			return T(max)
		}
		return T(int64Val)
	case protoreflect.Uint64Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed64Kind, protoreflect.Fixed32Kind:

		uint64Val := value.Uint()
		if uint64Val > uint64(max) {
			return T(max)
		}
		return T(uint64Val)
	case protoreflect.BoolKind:
		if value.Bool() {
			return T(1)
		}
		return T(0)
	default:
		return getDefaultInt[T](nullable)
	}
}

func getUIntSliceFromProto[T constraints.Unsigned](
	kind protoreflect.Kind,
	fieldVal protoreflect.List,
	max uint64,
) []T {
	arr := make([]T, 0)
	for i := 0; i < fieldVal.Len(); i++ {
		item := getUIntFromProto[T](kind, fieldVal.Get(i), false, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getUIntFromProto[T constraints.Unsigned](
	kind protoreflect.Kind,
	value protoreflect.Value,
	nullable bool,
	max uint64,
) interface{} {
	if !value.IsValid() {
		return getDefaultInt[T](nullable)
	}

	switch kind {
	case protoreflect.Uint64Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed64Kind, protoreflect.Fixed32Kind:
		uint64Val := value.Uint()
		if uint64Val > max {
			return T(max)
		}
		return T(uint64Val)
	case protoreflect.Int64Kind, protoreflect.Int32Kind,
		protoreflect.Sint64Kind, protoreflect.Sint32Kind,
		protoreflect.Sfixed64Kind, protoreflect.Sfixed32Kind:
		intV := value.Int()
		if intV < 0 {
			return getDefaultInt[T](nullable)
		}
		if uint64(intV) > max {
			return T(max)
		}
		return T(intV)
	case protoreflect.BoolKind:
		if value.Bool() {
			return T(1)
		}
		return T(0)
	default:
		return getDefaultInt[T](nullable)
	}
}

func getFloatSliceFromProto[T constraints.Float](
	kind protoreflect.Kind,
	fieldVal protoreflect.List,
	max float64,
) interface{} {
	arr := make([]T, 0)
	for i := 0; i < fieldVal.Len(); i++ {
		item := getFloatFromProto[T](kind, fieldVal.Get(i), false, max)
		arr = append(arr, item.(T))
	}

	return arr
}

func getFloatFromProto[T constraints.Float](
	kind protoreflect.Kind,
	value protoreflect.Value,
	nullable bool,
	max float64,
) interface{} {
	if !value.IsValid() {
		return getDefaultFloat[T](nullable)
	}

	if kind == protoreflect.DoubleKind || kind == protoreflect.FloatKind {
		float64Val := value.Float()
		if float64Val > max {
			return T(max)
		}
		return T(float64Val)
	}
	return getDefaultFloat[T](nullable)
}

func getProtoBool(kind protoreflect.Kind, value protoreflect.Value, nullable bool) interface{} {
	if kind == protoreflect.BoolKind {
		return value.Bool()
	}
	return getDefaultBool(nullable)
}

func getProtoDecimal(kind protoreflect.Kind, value protoreflect.Value, nullable bool) interface{} {
	switch kind {
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		dec, err := decimal.NewFromString(value.String())
		if err != nil {
			return getDefaultDecimal(nullable)
		}
		return dec
	default:
		return getDefaultDecimal(nullable)
	}
}

func getProtoDateTime(value protoreflect.Value, nullable bool) interface{} {
	timestampField, ok := value.Interface().(*dynamicpb.Message)
	if !ok {
		return getDefaultDateTime(nullable)
	}

	secondsField := timestampField.ProtoReflect().Descriptor().Fields().ByName("seconds")
	nanosField := timestampField.ProtoReflect().Descriptor().Fields().ByName("nanos")
	if secondsField == nil || nanosField == nil {
		return getDefaultDateTime(nullable)
	}

	seconds := timestampField.ProtoReflect().Get(secondsField)
	nanos := timestampField.ProtoReflect().Get(nanosField)

	timestamp := &timestamppb.Timestamp{
		Seconds: seconds.Int(),
		Nanos:   int32(nanos.Int()),
	}

	return timestamp.AsTime()
}

func getProtoString(value protoreflect.Value) string {
	return value.String()
}

func getDefaultString(nullable bool, defaultValue string) interface{} {
	if nullable {
		return nil
	}
	return defaultValue
}

// ProtoDeserializer represents a Protobuf deserializer.
type ProtoDeserializer struct {
	schemaRegistry   schemaregistry.Client
	baseDeserializer *serde.BaseDeserializer
	fileDescriptors  *fileDescriptorsCache
	topic            string
}

func (p *ProtoDeserializer) ToDynamicMessage(bytes []byte) (protoreflect.Message, error) {
	if len(bytes) == 0 {
		return nil, nil
	}

	var (
		key schemaKey
		err error
	)
	key.schemaID, err = getSchemaID(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema ID: %w", err)
	}
	var schemaInfo schemaregistry.SchemaInfo
	key.subject, err = p.baseDeserializer.SubjectNameStrategy(p.topic, p.baseDeserializer.SerdeType, schemaInfo)
	if err != nil {
		return nil, err
	}

	fileDesc, ok := p.fileDescriptors.get(key)
	if !ok {
		schemaInfo, err = p.baseDeserializer.Client.GetBySubjectAndID(key.subject, int(key.schemaID))
		if err != nil {
			return nil, fmt.Errorf("failed to get schema info: %w", err)
		}
		fileDesc, err = p.toFileDescriptor(schemaInfo)
		if err != nil {
			return nil, err
		}
		p.fileDescriptors.add(key, fileDesc)
	}

	bytesRead, msgIndexes, err := readMessageIndexes(bytes[5:])
	if err != nil {
		return nil, err
	}
	messageDesc, err := toMessageDescriptor(fileDesc, msgIndexes)
	if err != nil {
		return nil, err
	}

	msg := dynamicpb.NewMessage(messageDesc)
	if err := proto.Unmarshal(bytes[5+bytesRead:], msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value into protobuf message: %w", err)
	}

	return msg, nil
}

func (p *ProtoDeserializer) toFileDescriptor(info schemaregistry.SchemaInfo) (protoreflect.FileDescriptor, error) {
	// Resolve dependencies
	deps := make(map[string]string)
	err := serde.ResolveReferences(p.schemaRegistry, info, deps)
	if err != nil {
		return nil, err
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(filename string) (io.ReadCloser, error) {
				var schema string
				if filename == "." {
					schema = info.Schema
				} else {
					schema = deps[filename]
				}
				if schema == "" {
					return nil, fmt.Errorf("unknown reference: %s", filename)
				}
				return io.NopCloser(strings.NewReader(schema)), nil
			},
		}),
	}

	// Compile the .proto file
	fileDescriptors, err := compiler.Compile(context.Background(), ".")
	if err != nil {
		return nil, fmt.Errorf("failed to compile proto: %v", err)
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("could not resolve schema")
	}

	return fileDescriptors[0], nil
}

func getSchemaID(bytes []byte) (uint32, error) {
	if bytes[0] != magicByte {
		return 0, fmt.Errorf("unknown magic byte")
	}
	return binary.BigEndian.Uint32(bytes[1:5]), nil
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

func toMessageDescriptor(descriptor protoreflect.Descriptor, msgIndexes []int) (protoreflect.MessageDescriptor, error) {
	index := msgIndexes[0]
	if fileDesc, ok := descriptor.(protoreflect.FileDescriptor); ok {
		if len(msgIndexes) == 1 {
			return fileDesc.Messages().Get(index), nil
		}

		return toMessageDescriptor(fileDesc.Messages().Get(index), msgIndexes[1:])
	}

	if msgDesc, ok := descriptor.(protoreflect.MessageDescriptor); ok {
		if len(msgIndexes) == 1 {
			return msgDesc.Messages().Get(index), nil
		}

		return toMessageDescriptor(msgDesc.Messages().Get(index), msgIndexes[1:])
	}

	return nil, fmt.Errorf("unexpected type")
}

type schemaKey struct {
	subject  string
	schemaID uint32
}

type fileDescriptorsCache struct {
	values map[schemaKey]protoreflect.FileDescriptor
	lock   sync.RWMutex
}

func newFileDescriptorsCache() *fileDescriptorsCache {
	return &fileDescriptorsCache{
		values: make(map[schemaKey]protoreflect.FileDescriptor),
	}
}

func (p *fileDescriptorsCache) add(key schemaKey, value protoreflect.FileDescriptor) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.values[key] = value
}

func (p *fileDescriptorsCache) get(key schemaKey) (protoreflect.FileDescriptor, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	val, ok := p.values[key]
	return val, ok
}
